package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    private static final String SUBSCRIPTIONS = "subscriptions";

    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, Map<String, OwnedSubscription>> _cache;
    private final LoadingCache<String, Optional<OwnedSubscription>> _refreshCache;
    private final ListeningExecutorService _refreshService;
    private final CacheHandle _cacheHandle;
    private final Clock _clock;
    private final Meter _invalidationEventMeter;
    private final ReentrantLock _refreshLock = new ReentrantLock();
    private volatile ListenableFuture<?> _asyncRefreshFuture = null;
    private volatile long _lastInvalidationTimestamp;

    @Inject
    public CachingSubscriptionDAO(@CachingSubscriptionDAODelegate SubscriptionDAO delegate,
                                  @CachingSubscriptionDAORegistry CacheRegistry cacheRegistry,
                                  @CachingSubscriptionDAOExecutorService ListeningExecutorService refreshService,
                                  MetricRegistry metricRegistry, Clock clock) {
        _delegate = checkNotNull(delegate, "delegate");
        _refreshService = checkNotNull(refreshService, "refreshService");
        _clock = checkNotNull(clock, "clock");
        _lastInvalidationTimestamp = clock.millis();

        Ticker ticker = ClockTicker.getTicker(clock);

        // The subscription cache has only a single value.  Use it for (a) expiration, (b) dropwizard cache clearing.
        _cache = CacheBuilder.newBuilder().
                refreshAfterWrite(10, TimeUnit.MINUTES).
                ticker(ticker).
                recordStats().
                removalListener((RemovalListener<String, Map<String, OwnedSubscription>>) notification -> {
                    if (notification.getCause() == RemovalCause.EXPLICIT) {
                        // Subscriptions were explicitly removed due to an invalidation event
                        _lastInvalidationTimestamp = _clock.millis();
                    }
                }).
                build(new CacheLoader<String, Map<String, OwnedSubscription>>() {
                    /**
                     * Synchronously load the subscription map.  This is called to initialize the subscriptions or on
                     * the first {@link LoadingCache#getUnchecked(Object)} call after an invalidation event.
                     */
                    @Override
                    public Map<String, OwnedSubscription> load(String ignored) throws Exception {
                        return indexByName(_delegate.getAllSubscriptions());
                    }

                    /**
                     * Override to allow background refreshes when either {@link CacheBuilder#refreshAfterWrite(long, TimeUnit)}
                     * expires or {@link LoadingCache#refresh(Object)} is called.  While refreshing asynchronously the
                     * currently cached subscription map will continue to be served without blocking.  The implementation
                     * defers to {@link #load(String)} but executes it in another thread.
                     */
                    @Override
                    public ListenableFuture<Map<String, OwnedSubscription>> reload(String key, Map<String, OwnedSubscription> oldValue)
                            throws Exception {
                        return refreshService.submit(() -> load(key));
                    }
                });
        _cacheHandle = cacheRegistry.register("subscriptions", _cache, true);

        // Depending on the number of subscriptions the time to reload them all can take hundreds of milliseconds.
        // This means that the time to load a single subscription in getSubscription() may unnecessarily block for that
        // time.  The following will cache single-subscriptions short-term while the full cache is replenished
        // asynchronously.

        _refreshCache = CacheBuilder.newBuilder().
                expireAfterWrite(1, TimeUnit.SECONDS).
                softValues().
                ticker(ticker).
                build(new CacheLoader<String, Optional<OwnedSubscription>>() {
                    @Override
                    public Optional<OwnedSubscription> load(String subscription) throws Exception {
                        // Caches don't support null values, so use empty Optionals for nulls
                        return Optional.ofNullable(_delegate.getSubscription(subscription));
                    }
                });

        _invalidationEventMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "invalidation-events"));
    }

    private Map<String, OwnedSubscription> indexByName(Collection<OwnedSubscription> subscriptions) {
        return Maps.uniqueIndex(subscriptions, new Function<Subscription, String>() {
            @Override
            public String apply(Subscription subscription) {
                return subscription.getName();
            }
        });
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                                   Duration eventTtl) {
        // Get the cached subscription, if it exists.  Don't call getSubscription() because that may return a cached
        // value from _refreshCache and we need to be sure the existing subscription is current as of existingReadTimestamp.
        long existingReadTimestamp = _clock.millis();
        OwnedSubscription existing;
        Map<String, OwnedSubscription> subscriptions = _cache.getIfPresent(SUBSCRIPTIONS);
        if (subscriptions != null) {
            existing = subscriptions.get(subscription);
        } else {
            existing = _delegate.getSubscription(subscription);
        }

        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        if (shouldInvalidateAfterInsert(existing, existingReadTimestamp, tableFilter, subscriptionTtl, eventTtl)) {
            // Synchronously tell every other server in the cluster to forget what it has cached about subscriptions.
            _invalidationEventMeter.mark();
            _cacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
        }
    }

    private boolean shouldInvalidateAfterInsert(@Nullable OwnedSubscription existing, long existingReadTimestamp,
                                                Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        if (existing == null) {
            // This is a new subscription so we must invalidate to force a reload
            return true;
        }

        // If there has been an invalidation event since the existing subscription was read then its state may be out
        // of sync, so we must invalidate to avoid an inconsistent cache state.
        if (existingReadTimestamp < _lastInvalidationTimestamp) {
            return true;
        }

        // If the filter or event TTL changed the the update must be propagated immediately.
        if (!Objects.equal(existing.getEventTtl(), eventTtl) || !Objects.equal(existing.getTableFilter(), tableFilter)) {
            return true;
        }

        // All subscriptions get refreshed every 10 minutes, so if the subscription TTL doesn't affect whether it expires
        // in the next 10 minutes then don't invalidate all subscriptions.  Since the cache is reloaded asynchronously
        // use 11 minutes to provide ample reload time.
        if (subscriptionTtl.isShorterThan(Duration.standardMinutes(10)) ||
                existing.getExpiresAt().before(new DateTime(_clock.millis()).plusMinutes(11).toDate())) {
            return true;
        }

        // At this point the cached view of the subscription is accurate enough for use until the cache is refreshed
        // in no later than 10 minutes.
        return false;
    }

    @Override
    public void deleteSubscription(String subscription) {
        _delegate.deleteSubscription(subscription);

        // Synchronously tell every other server in the cluster to forget what it has cached about subscriptions.
        _cacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        // Do not block reloading all subscriptions to only return a single subscription.  This should only
        // happen while the subscriptions are being fully reloaded following an invalidation event.
        Map<String, OwnedSubscription> subscriptions = _cache.getIfPresent(SUBSCRIPTIONS);
        if (subscriptions != null) {
            OwnedSubscription ownedSubscription = subscriptions.get(subscription);
            // If the expiration date is in the past then we need to go to the delegate to ensure it still exists.
            // This can happen if the subscription expired but no subscription reads occurred in over 10 minutes,
            // since the cache will return a dirty value while the subscription cache is updated asynchronously.
            // This scenario is pretty unlikely but should be handled properly just the same.
            // TODO:  BJK:  Make sure there is a unit test specifically for this scenario
            if (ownedSubscription == null || ownedSubscription.getExpiresAt().getTime() > _clock.millis()) {
                return ownedSubscription;
            }
        }

        // Spawn an asynchronous refresh if one has not already been requested recently
        refreshSubscriptionsAsync();

        // Until the refresh completes return the subscription from the delegate.
        return _refreshCache.getUnchecked(subscription).orElse(null);
    }

    private void refreshSubscriptionsAsync() {
        // Do not refresh again if another refresh is underway
        if (_asyncRefreshFuture == null) {
            // Do not block acquiring the refresh lock.  If we don't get it that only means another thread is already
            // doing the same work this thread would be doing.
            if (_refreshLock.tryLock()) {
                try {
                    if (_asyncRefreshFuture == null) {
                        // _cache.refresh(SUBSCRIPTIONS) only runs asynchronously if the current value exists and
                        // is being replaced.  In this case there is no current value, either because it has never
                        // been loaded or it was invalidated.  Therefore we need to explicitly refresh the cache
                        // asynchronously, otherwise the refresh call would block synchronously.
                        _asyncRefreshFuture = _refreshService.submit(() -> {
                            try {
                                _cache.get(SUBSCRIPTIONS);
                            } catch (Throwable t) {
                                // Whatever the problem was it'll be caught and propagated on a future synchronous call
                                // to get subscriptions if it wasn't transient, so just log this one.
                                LoggerFactory.getLogger(getClass()).debug("Failed to asynchronously update all subscriptions", t.getCause());
                            } finally {
                                // Reset the future to allow more refreshes if necessary
                                _asyncRefreshFuture = null;
                            }
                        });
                    }
                } finally {
                    _refreshLock.unlock();
                }
            }
        }
    }

    @Override
    public Collection<OwnedSubscription> getAllSubscriptions() {
        return _cache.getUnchecked(SUBSCRIPTIONS).values();
    }
}
