package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import org.joda.time.Duration;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class CachingSubscriptionDAOTest {

    private Clock _clock;
    private ListeningExecutorService _service;
    private SubscriptionDAO _delegate;
    private CachingSubscriptionDAO _cachingSubscriptionDAO;
    private Cache<String, ?> _cache;

    @BeforeMethod
    public void setUp() {
        _clock = mock(Clock.class);
        _service = mock(ListeningExecutorService.class);
        _delegate = new InMemorySubscriptionDAO(_clock);

        // Insert some test data into the delegate
        for (int i=0; i < 3; i++) {
            _delegate.insertSubscription("owner", "sub" + i, Conditions.alwaysTrue(), Duration.standardDays(1), Duration.standardMinutes(5));
        }

        CacheRegistry cacheRegistry = mock(CacheRegistry.class);

        _cachingSubscriptionDAO = new CachingSubscriptionDAO(_delegate, cacheRegistry, _service, _clock);

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
        when(_clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(10));

        // Cached values should still be returned
        assertEquals(subscriptions.stream().map(Subscription::getName).sorted().collect(Collectors.toList()),
                ImmutableList.of("sub0", "sub1", "sub2"));

        verify(_service, never()).submit(any(Callable.class));

        // Move time forward to just over 10 minutes
        when(_clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(10) + 1);

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
}
