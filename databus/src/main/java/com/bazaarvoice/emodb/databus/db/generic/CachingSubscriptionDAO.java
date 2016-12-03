package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.joda.time.Duration;

import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    private static final String SUBSCRIPTIONS = "subscriptions";

    // Loading cache cannot have null values, so use a single dummy value as a stand-in when a subscription does not exist.
    private static final OwnedSubscription NULL_SUBSCRIPTION =
            new DefaultOwnedSubscription("__null", Conditions.alwaysFalse(), new Date(0), Duration.ZERO, "__null");


    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, OwnedSubscription> _subscriptionCache;
    private final LoadingCache<String, List<OwnedSubscription>> _allSubscriptionsCache;
    private final ListeningExecutorService _refreshService;
    private final CacheHandle _subscriptionCacheHandle;
    private final Meter _invalidationEventMeter;

    /**
     * In a prior version of Emo all subscriptions were maintained in a single cache entry.
     * In order to support an in-flight upgrade to the current system where each subscription is cached independently
     * the following cache and cache handle are necessary.  Otherwise, cache invalidations from the legacy system
     * would not be handled on the current system, nor vice versa.
     *
     * Once the entire cluster has been upgraded and there are no more legacy instances active the following
     * legacy attributes and all code referencing them should be removed.
     */
    private final LoadingCache<String, Map<String, OwnedSubscription>> _legacyCache;
    private final CacheHandle _legacyCacheHandle;

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
                    if (notification.getCause() == RemovalCause.EXPLICIT && notification.getValue() != NULL_SUBSCRIPTION) {
                        _allSubscriptionsCache.invalidate(SUBSCRIPTIONS);
                    }
                })
                .build(new CacheLoader<String, OwnedSubscription>() {
                    @Override
                    public OwnedSubscription load(String subscription) throws Exception {
                        OwnedSubscription ownedSubscription = _delegate.getSubscription(subscription);
                        if (ownedSubscription == null) {
                            // Can't cache null, use special null value
                            ownedSubscription = NULL_SUBSCRIPTION;
                        }

                        // Ensure there is an entry in the legacy cache so it can receive invalidation events.
                        _legacyCache.get(SUBSCRIPTIONS);

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

        // Register the legacy cache in order to exchange cache invalidations with the legacy system.
        _legacyCache = CacheBuilder.newBuilder()
                .removalListener(notification -> {
                    // The legacy system sends a single notification when any subscription changes.  Without knowing
                    // which subscription changed the only safe action is to invalidate them all.  This is inefficient
                    // but is necessary only for the brief period after the new stack is deployed and the legacy
                    // stack hasn't yet been torn down.
                    _subscriptionCache.invalidateAll();
                })
                .build(new CacheLoader<String, Map<String, OwnedSubscription>>() {
                    @Override
                    public Map<String, OwnedSubscription> load(String key) throws Exception {
                        // The actual cached object doesn't matter since this cache is only used for receiving
                        // invalidation messages.  Just need to provide a non-null value.
                        return ImmutableMap.of();
                    }
                });
        _legacyCacheHandle = cacheRegistry.register("subscriptions", _legacyCache, true);
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

        // Send notification to the legacy stack that all subscriptions must be invalidated since it only supports
        // full cache invalidation.
        _legacyCacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        // Try to load the subscription without forcing a synchronous reload.  This will return null only if it is the
        // first time the subscription has been loaded or if it has been invalidated.  The returned subscription may
        // be expired but the cache registry will have invalidated the cached value if it were changed -- the reload is
        // only used as a failsafe.  If the value is expired the cache will asynchronously reload it in the background.

        OwnedSubscription ownedSubscription = _subscriptionCache.getIfPresent(subscription);
        if (ownedSubscription == null) {
            // This time call get() to force the value to load, possibly synchronously.  This will also cause the value
            // to be cached.

            ownedSubscription = _subscriptionCache.getUnchecked(subscription);
        }

        // If the subscription did not exist return null and immediately remove from cache.
        if (ownedSubscription == NULL_SUBSCRIPTION) {
            _subscriptionCache.invalidate(subscription);
            ownedSubscription = null;
        }

        return ownedSubscription;
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
