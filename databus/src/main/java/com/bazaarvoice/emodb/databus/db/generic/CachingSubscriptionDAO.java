package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.google.common.base.Function;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collection;
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
    private final ReentrantLock _refreshLock = new ReentrantLock();
    private volatile ListenableFuture<?> _asyncRefreshFuture = null;

    @Inject
    public CachingSubscriptionDAO(@CachingSubscriptionDAODelegate SubscriptionDAO delegate,
                                  @CachingSubscriptionDAORegistry CacheRegistry cacheRegistry,
                                  @CachingSubscriptionDAOExecutorService ListeningExecutorService refreshService,
                                  Clock clock) {
        _delegate = checkNotNull(delegate, "delegate");
        _refreshService = checkNotNull(refreshService, "refreshService");

        Ticker ticker = ClockTicker.getTicker(clock);

        // The subscription cache has only a single value.  Use it for (a) expiration, (b) dropwizard cache clearing.
        _cache = CacheBuilder.newBuilder().
                refreshAfterWrite(10, TimeUnit.MINUTES).
                ticker(ticker).
                recordStats().
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
        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        // Synchronously tell every other server in the cluster to forget what it has cached about subscriptions.
        _cacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
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
            return subscriptions.get(subscription);
        }

        // Spawn an asynchronous refresh if one has not already been requested recently
        refreshSubscriptionsAsync();

        // While until the refresh completes return the subscription from the delegate.
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
