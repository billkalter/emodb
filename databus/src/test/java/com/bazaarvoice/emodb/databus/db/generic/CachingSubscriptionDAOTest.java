package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Conditions;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@SuppressWarnings("unchecked")
public class CachingSubscriptionDAOTest {

    private DateTime _now;
    private ListeningExecutorService _service;
    private SubscriptionDAO _delegate;
    private CachingSubscriptionDAO _cachingSubscriptionDAO;
    private Cache<String, ?> _cache;
    private CacheHandle _cacheHandle;

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
        when(cacheRegistry.register(eq("subscriptions"), any(Cache.class), eq(true))).thenReturn(_cacheHandle);

        _cachingSubscriptionDAO = new CachingSubscriptionDAO(_delegate, cacheRegistry, _service, clock);

        ArgumentCaptor<Cache> cacheCaptor = ArgumentCaptor.forClass(Cache.class);
        verify(cacheRegistry).register(eq("subscriptions"), cacheCaptor.capture(), eq(true));
        _cache = cacheCaptor.getValue();
    }

    @Test
    public void testColdReadAllSubscriptions() throws Exception {
        Collection<OwnedSubscription> subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        // No asynchronous loading should have taken place
        verify(_service, never()).submit(any(Callable.class));
    }

    @Test
    public void testReadAllSubscriptionsAfterInvalidate() throws Exception {
        Collection<OwnedSubscription> subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        // Remove sub2 from the delegate
        _delegate.deleteSubscription("sub2");

        // Cached values should still be returned
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        // Invalidate the cache
        _cache.invalidateAll();

        // Reading again should exclude sub2
        subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1"));

        // No asynchronous loading should have taken place
        verify(_service, never()).submit(any(Callable.class));
    }

    @Test
    public void testReadAllSubscriptionsAfterExpiration() throws Exception {
        Collection<OwnedSubscription> subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        // Remove sub2 from the delegate
        _delegate.deleteSubscription("sub2");

        // Move time forward exactly 10 minutes
        _now = _now.plusMinutes(10);

        // Cached values should still be returned
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        verify(_service, never()).submit(any(Callable.class));

        // Move time forward to just over 10 minutes
        _now = _now.plusMillis(1);

        SettableFuture future = SettableFuture.create();
        when(_service.submit(any(Callable.class))).thenReturn(future);

        // Reading again should still return the old value but spawn an asynchronous reload
        subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        ArgumentCaptor<Callable> callableCaptor = ArgumentCaptor.forClass(Callable.class);
        verify(_service).submit(callableCaptor.capture());

        // Let the callable execute
        future.set(callableCaptor.getValue().call());

        // Reading again now should return the updated value
        subscriptions = _cachingSubscriptionDAO.getAllSubscriptions();
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1"));

        // Verify the asynchronous update was made only once and not again on the second call to get all subscriptions
        verify(_service, times(1)).submit(any(Callable.class));
    }

    @Test
    public void testColdReadSingleSubscription() throws Exception {
        OwnedSubscription subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertEquals(subscription.getName(), "sub0");

        // Verify a call was made to refresh the full subscription cache
        verify(_service).submit(any(Runnable.class));
    }

    @Test
    public void testReadSingleSubscriptionWithAllSubscriptionsCached() throws Exception {
        // Cause all subscriptions to be cached
        _cachingSubscriptionDAO.getAllSubscriptions();
        // With all subscriptions cached the following should read the value from cache
        OwnedSubscription subscription = _cachingSubscriptionDAO.getSubscription("sub0");
        assertEquals(subscription.getName(), "sub0");

        // No asynchronous loading should have taken place
        verify(_service, never()).submit(any(Callable.class));
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
        verify(_cacheHandle).invalidate(InvalidationScope.DATA_CENTER, "subscriptions");
    }

    @Test
    public void testInvalidateOnDelete() throws Exception {
        _cachingSubscriptionDAO.deleteSubscription("sub0");
        verify(_cacheHandle).invalidate(InvalidationScope.DATA_CENTER, "subscriptions");
    }

}
