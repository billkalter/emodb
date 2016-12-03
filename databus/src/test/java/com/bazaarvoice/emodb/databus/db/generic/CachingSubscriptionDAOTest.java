package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@SuppressWarnings("unchecked")
public class CachingSubscriptionDAOTest {

    private DateTime _now;
    private ListeningExecutorService _service;
    private SubscriptionDAO _delegate;
    private CachingSubscriptionDAO _cachingSubscriptionDAO;
    private Cache<String, ?> _cache;
    private Cache<String, ?> _legacyCache;
    private CacheHandle _cacheHandle;
    private CacheHandle _legacyCacheHandle;

    @BeforeMethod
    public void setUp() {
        _now = new DateTime(2016, 1, 1, 0, 0, DateTimeZone.UTC);

        Clock clock = mock(Clock.class);
        when(clock.millis()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                return _now.getMillis();
            }
        });

        _service = mock(ListeningExecutorService.class);
        _delegate = new InMemorySubscriptionDAO(clock);

        // Insert some test data into the delegate
        for (int i=0; i < 3; i++) {
            _delegate.insertSubscription("owner", "sub" + i, Conditions.alwaysTrue(), Duration.standardDays(1), Duration.standardMinutes(5));
        }

        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        _cacheHandle = mock(CacheHandle.class);
        when(cacheRegistry.register(eq("subscriptionsByName"), any(Cache.class), eq(true))).thenReturn(_cacheHandle);
        // Make sure the legacy cache handle registration doesn't return null
        _legacyCacheHandle = mock(CacheHandle.class);
        when(cacheRegistry.register(eq("subscriptions"), any(Cache.class), eq(true))).thenReturn(_legacyCacheHandle);

        _cachingSubscriptionDAO = new CachingSubscriptionDAO(_delegate, cacheRegistry, _service, new MetricRegistry(), clock);

        ArgumentCaptor<Cache> cacheCaptor = ArgumentCaptor.forClass(Cache.class);
        verify(cacheRegistry).register(eq("subscriptionsByName"), cacheCaptor.capture(), eq(true));
        _cache = cacheCaptor.getValue();


        cacheCaptor = ArgumentCaptor.forClass(Cache.class);
        verify(cacheRegistry).register(eq("subscriptions"), cacheCaptor.capture(), eq(true));
        _legacyCache = cacheCaptor.getValue();
    }

    @AfterMethod
    public void verifyMocks() {
        verifyNoMoreInteractions(_cacheHandle, _legacyCacheHandle, _service);
    }

    @Test
    public void testColdReadAllSubscriptions() throws Exception {
        Collection<OwnedSubscription> subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));
    }

    @Test
    public void testReadAllSubscriptionsAfterInvalidate() throws Exception {
        Collection<OwnedSubscription> subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        // Update sub2 with a new condition on the delegate
        Condition sub2Condition = Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("invalidate2"));
        _delegate.insertSubscription("owner", "sub2", sub2Condition, Duration.standardDays(1), Duration.standardMinutes(5));

        // Invalidate sub2
        _cache.invalidate("sub2");

        // Reading again should include the updated sub2
        subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));
        assertEquals(subscriptions.stream()
                        .sorted((left, right) -> left.getName().compareTo(right.getName()))
                        .map(Subscription::getTableFilter)
                        .collect(Collectors.toList()),
                ImmutableList.of(Conditions.alwaysTrue(), Conditions.alwaysTrue(), sub2Condition));
    }

    @Test
    public void testReadAllSubscriptionsAfterExpiration() throws Exception {
        // In order for this test to run deterministically we must have only one subscription.  Delete all but sub0.
        _delegate.deleteSubscription("sub1");
        _delegate.deleteSubscription("sub2");

        List<OwnedSubscription> subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), "sub0");
        assertEquals(subscriptions.get(0).getTableFilter(), Conditions.alwaysTrue());

        // Update sub0 with a new condition on the delegate
        Condition newCondition = Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("invalidate2"));
        _delegate.insertSubscription("owner", "sub0", newCondition, Duration.standardDays(1), Duration.standardMinutes(5));

        // Move time forward exactly 10 minutes
        _now = _now.plusMinutes(10);

        // Cached subscription should still be returned
        subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), "sub0");
        assertEquals(subscriptions.get(0).getTableFilter(), Conditions.alwaysTrue());

        // No asynchronous reload should have been spawned
        verify(_service, never()).submit(any(Callable.class));

        // Move time forward to just over 10 minutes
        _now = _now.plusMillis(1);

        SettableFuture future = SettableFuture.create();
        when(_service.submit(any(Callable.class))).thenReturn(future);

        // Reading again should still return the old value but spawn an asynchronous reload of the subscription
        subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), "sub0");
        assertEquals(subscriptions.get(0).getTableFilter(), Conditions.alwaysTrue());

        ArgumentCaptor<Callable> callableCaptor = ArgumentCaptor.forClass(Callable.class);
        verify(_service).submit(callableCaptor.capture());

        // Verify the callable returns the updated value
        OwnedSubscription newSub0 = (OwnedSubscription) callableCaptor.getValue().call();
        assertEquals(newSub0.getTableFilter(), newCondition);
        future.set(newSub0);

        // Normally when the subscription changed the cache handler would have invalidated the subscription.
        // This is necessary because without an explicit cache invalidation the all-subscriptions cache value
        // will not be updated.  So invalidate it now.
        _cache.invalidate("sub0");

        // Reading again now should return the updated value
        subscriptions = ImmutableList.copyOf(_cachingSubscriptionDAO.getAllSubscriptions());
        assertEquals(subscriptions.size(), 1);
        assertEquals(subscriptions.get(0).getName(), "sub0");
        assertEquals(subscriptions.get(0).getTableFilter(), newCondition);
    }

    @Test
    public void testColdReadSingleSubscription() throws Exception {
        OwnedSubscription subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertEquals(subscription.getName(), "sub0");
    }

    @Test
    public void testReadSingleSubscriptionWithAllSubscriptionsCached() throws Exception {
        // Cause all subscriptions to be cached
        _cachingSubscriptionDAO.getAllSubscriptions();
        // With all subscriptions cached the following should read the value from cache
        OwnedSubscription subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertEquals(subscription.getName(), "sub0");
    }

    @Test
    public void testReadSingleSubscriptionAfterExpiration() throws Exception {
        SettableFuture future = SettableFuture.create();
        when(_service.submit(any(Callable.class))).thenReturn(future);

        // Cause all subscriptions to be cached
        _cachingSubscriptionDAO.getAllSubscriptions();
        // Move time forward one hour
        _now = _now.plusHours(1);
        // Remove sub0 from the delegate
        _delegate.deleteSubscription("sub0");

        // Read the subscription.  This should return the cached value and spawn an asynchronous refresh
        OwnedSubscription subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertEquals(subscription.getName(), "sub0");

        // Verify a call was made to refresh the full subscription cache
        ArgumentCaptor<Callable> callableCaptor = ArgumentCaptor.forClass(Callable.class);
        verify(_service).submit(callableCaptor.capture());

        // Let the callable execute
        future.set(callableCaptor.getValue().call());

        // Reading the subscription now should correctly return null.
        subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertNull(subscription);
    }

    @Test
    public void testInvalidateOnInsert() throws Exception {
        _cachingSubscriptionDAO.insertSubscription("owner", "sub4", Conditions.alwaysTrue(), Duration.standardDays(1), Duration.standardMinutes(5));
        verify(_cacheHandle).invalidate(InvalidationScope.DATA_CENTER, "sub4");

        // Legacy cache handle should also receive invalidation
        verify(_legacyCacheHandle).invalidate(InvalidationScope.DATA_CENTER, "subscriptions");
    }

    @Test
    public void testInvalidateOnDelete() throws Exception {
        _cachingSubscriptionDAO.deleteSubscription("sub0");
        verify(_cacheHandle).invalidate(InvalidationScope.DATA_CENTER, "sub0");

        // Legacy cache handle should also receive invalidation
        verify(_legacyCacheHandle).invalidate(InvalidationScope.DATA_CENTER, "subscriptions");
    }

    @Test
    public void testLegacyInvalidation() throws Exception {
        // Cause all subscriptions to be cached
        _cachingSubscriptionDAO.getAllSubscriptions();

        // Remove sub0 from the delegate
        _delegate.deleteSubscription("sub0");

        // Verify the cached sub0 is still returned
        assertNotNull(_cachingSubscriptionDAO.getSubscription("sub0"));

        // Simulate receiving a legacy invalidation notification
        _legacyCache.invalidate("subscriptions");

        // Cache should have been invalidated, so now sub0 will not exist
        assertNull(_cachingSubscriptionDAO.getSubscription("sub0"));
    }
}
