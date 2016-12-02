package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.joda.time.Duration;

import java.time.Clock;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    private static final String SUBSCRIPTIONS = "subscriptions";

    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, OwnedSubscription> _subscriptionCache;
    private final LoadingCache<String, List<OwnedSubscription>> _allSubscriptionsCache;
    private final ListeningExecutorService _refreshService;
    private final CacheHandle _subscriptionCacheHandle;
    private final Meter _invalidationEventMeter;

    @Inject
    public CachingSubscriptionDAO(@CachingSubscriptionDAODelegate SubscriptionDAO delegate,
                                  @CachingSubscriptionDAORegistry CacheRegistry cacheRegistry,
                                  @CachingSubscriptionDAOExecutorService ListeningExecutorService refreshService,
                                  MetricRegistry metricRegistry, Clock clock) {
        _delegate = checkNotNull(delegate, "delegate");
        _refreshService = checkNotNull(refreshService, "refreshService");

        Ticker ticker = ClockTicker.getTicker(clock);

        // The all subscription cache is only used to track the set of all subscriptions and only has a single value.
        _allSubscriptionsCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .recordStats()
                .build(new CacheLoader<String, List<OwnedSubscription>>() {
                    @Override
                    public List<OwnedSubscription> load(String key) throws Exception {
                        Iterable<String> subscriptionNames = _delegate.getAllSubscriptionNames();
                        List<OwnedSubscription> subscriptions = Lists.newArrayList();

                        for (String name : subscriptionNames) {
                            OwnedSubscription subscription = getSubscription(name);
                            if (subscription != null) {
                                subscriptions.add(subscription);
                            }
                        }

                        return subscriptions;
                    }

                });

        _subscriptionCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .removalListener((RemovalListener<String, OwnedSubscription>) notification -> {
                    // If the subscription was removed due to an explicit invalidation then also invalidate
                    // the list of all subscriptions.
                    if (notification.getCause() == RemovalCause.EXPLICIT) {
                        _allSubscriptionsCache.invalidate(SUBSCRIPTIONS);
                    }
                })
                .build(new CacheLoader<String, OwnedSubscription>() {
                    @Override
                    public OwnedSubscription load(String subscription) throws Exception {
                        OwnedSubscription ownedSubscription = _delegate.getSubscription(subscription);
                        if (ownedSubscription == null) {
                            throw new NoSuchElementException(subscription);
                        }
                        return ownedSubscription;
                    }

                    /**
                     * When the cached value is reloaded due to {@link CacheBuilder#refreshAfterWrite(long, TimeUnit)}
                     * having expired reload the value asynchronously.
                     */
                    @Override
                    public ListenableFuture<OwnedSubscription> reload(String key, OwnedSubscription oldValue)
                            throws Exception {
                        return _refreshService.submit(() -> load(key));
                    }
                });

        _subscriptionCacheHandle = cacheRegistry.register("subscriptionsByName", _subscriptionCache, true);

        _invalidationEventMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "invalidation-events"));
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                                   Duration eventTtl) {
        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        // Invalidate this subscription.  No need to invalidate the list of all subscriptions since this will happen
        // naturally in _subscriptionCache's removal listener.
        invalidateSubscription(subscription);
    }

    @Override
    public void deleteSubscription(String subscription) {
        _delegate.deleteSubscription(subscription);

        // Synchronously tell every other server in the cluster to forget what it has cached about the subscription.
        invalidateSubscription(subscription);
    }

    private void invalidateSubscription(String subscription) {
        _invalidationEventMeter.mark();
        _subscriptionCacheHandle.invalidate(InvalidationScope.DATA_CENTER, subscription);
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        // Try to load the subscription without forcing a synchronous reload.  This will return null only if it is the
        // first time the subscription has been loaded or if it has been invalidated.  The returned subscription may
        // be dirty from the cache's perspective, but the cache handler will have invalidated the cached value if it
        // were changed -- the reload is only used as a failsafe.  If the value is dirty the cache will asynchronously
        // reload it in the background.

        OwnedSubscription ownedSubscription = _subscriptionCache.getIfPresent(subscription);
        if (ownedSubscription != null) {
            return ownedSubscription;
        }

        // This time call get() to force the value to load, possibly synchronously.  This will also cause the value
        // to be cached.
        try {
            return _subscriptionCache.get(subscription);
        } catch (ExecutionException e) {
            // Loading caches cannot store nulls.  If the subscription did not exist the loader would have thrown
            // NoSuchElementException.  Check if that is the case, otherwise propagate the exception.
            if (e.getCause() instanceof NoSuchElementException) {
                return null;
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Iterable<OwnedSubscription> getAllSubscriptions() {
        return _allSubscriptionsCache.getUnchecked(SUBSCRIPTIONS);
    }

    @Override
    public Iterable<String> getAllSubscriptionNames() {
        return Iterables.transform(getAllSubscriptions(), OwnedSubscription::getName);
    }
}
