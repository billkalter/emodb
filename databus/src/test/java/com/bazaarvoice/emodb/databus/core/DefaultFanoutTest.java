package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.table.db.Table;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@SuppressWarnings("unchecked")
public class DefaultFanoutTest {

    private DefaultFanout _defaultFanout;
    private Supplier<Iterable<OwnedSubscription>> _subscriptionsSupplier;
    private DataCenter _currentDataCenter;
    private DataCenter _remoteDataCenter;
    private DataProvider _dataProvider;
    private DatabusAuthorizer _databusAuthorizer;
    private String _remoteChannel;
    private Multimap<String, ByteBuffer> _eventsSinked;
    private ExecutorService _fanoutThreads;
    private Clock _clock;

    @BeforeMethod
    private void setUp() {
        _eventsSinked = ArrayListMultimap.create();

        Function<Multimap<String, ByteBuffer>, Void> eventSink = new Function<Multimap<String, ByteBuffer>, Void>() {
            @Override
            public Void apply(Multimap<String, ByteBuffer> input) {
                _eventsSinked.putAll(input);
                return null;
            }
        };

        _subscriptionsSupplier = mock(Supplier.class);
        _currentDataCenter = mock(DataCenter.class);
        when(_currentDataCenter.getName()).thenReturn("local");
        _remoteDataCenter = mock(DataCenter.class);
        when(_remoteDataCenter.getName()).thenReturn("remote");
        _remoteChannel = ChannelNames.getReplicationFanoutChannel(_remoteDataCenter);

        RateLimitedLogFactory rateLimitedLogFactory = mock(RateLimitedLogFactory.class);
        when(rateLimitedLogFactory.from(any(Logger.class))).thenReturn(mock(RateLimitedLog.class));

        _dataProvider = mock(DataProvider.class);
        _databusAuthorizer = mock(DatabusAuthorizer.class);

        SubscriptionEvaluator subscriptionEvaluator = new SubscriptionEvaluator(
                _dataProvider, _databusAuthorizer, rateLimitedLogFactory);

        // By default the fanout thread service will execute the callable in-thread.
        _fanoutThreads = mock(ExecutorService.class);
        when(_fanoutThreads.submit(any(Callable.class))).thenAnswer(
                invocation -> Futures.immediateCheckedFuture(((Callable) invocation.getArguments()[0]).call()));

        // By default the mock clock acts like a system clock
        _clock = mock(Clock.class);
        when(_clock.millis()).thenAnswer(ignore -> System.currentTimeMillis());
        
        _defaultFanout = new DefaultFanout("test", mock(EventSource.class), eventSink, true, Duration.standardSeconds(1),
                _subscriptionsSupplier, _currentDataCenter, rateLimitedLogFactory, subscriptionEvaluator,
                _fanoutThreads, new MetricRegistry(), _clock);
    }

    @Test
    public void testMatchingTable() {
        addTable("matching-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("matching-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "matching-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(true);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        assertEquals(_eventsSinked,
                ImmutableMultimap.of("test", event.getData(), _remoteChannel, event.getData()));
    }

    @Test
    public void testNotMatchingTable() {
        addTable("other-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("not-matching-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "other-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(true);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        // Event does not match subscription, should only go to remote fanout
        assertEquals(_eventsSinked,
                ImmutableMultimap.of(_remoteChannel, event.getData()));
    }

    @Test
    public void testUnauthorizedFanout() {
        addTable("unauthorized-table");

        OwnedSubscription subscription = new DefaultOwnedSubscription(
                "test", Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("unauthorized-table")),
                new Date(), Duration.standardDays(1), "owner0");

        EventData event = newEvent("id0", "unauthorized-table", "key0");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of(subscription));
        DatabusAuthorizer.DatabusAuthorizerByOwner authorizerByOwner = mock(DatabusAuthorizer.DatabusAuthorizerByOwner.class);
        when(authorizerByOwner.canReceiveEventsFromTable("matching-table")).thenReturn(false);
        when(_databusAuthorizer.owner("owner0")).thenReturn(authorizerByOwner);

        _defaultFanout.copyEvents(ImmutableList.of(event));

        // Event is not authorized for owner, should only go to remote fanout
        assertEquals(_eventsSinked,
                ImmutableMultimap.of(_remoteChannel, event.getData()));

    }

    @Test
    public void testFanoutMultiThreading() {
        addTable("fanout-table");

        when(_subscriptionsSupplier.get()).thenReturn(ImmutableList.of());

        // Set the clock to a day in the future.  This will force the fanout to re-evaluate lag
        DateTime now = DateTime.now().plus(Duration.standardDays(1));
        when(_clock.millis()).thenReturn(now.getMillis());

        // Put an event in the queue that is just above the 15 minute threshold for parallelization
        EventData event = newEventAtTime("id0", "fanout-table", "key0", now.minus(Duration.standardMinutes(15)).getMillis());

        _defaultFanout.copyEvents(ImmutableList.of(event));

        Multimap<String, Object> expectedEvents = ArrayListMultimap.create();
        expectedEvents.put(_remoteChannel, event.getData());
        assertEquals(_eventsSinked, expectedEvents);

        // Move time forward one minute
        when(_clock.millis()).thenReturn((now = now.plus(Duration.standardMinutes(1))).getMillis());

        // Add 1000 more events
        List<EventData> events = Lists.newArrayListWithCapacity(1000);
        _eventsSinked.clear();
        expectedEvents.clear();
        for (int i=1; i <= 1000; i++) {
            event = newEventAtTime("id" + i, "fanout-table", "key" + i, now.minus(Duration.standardMinutes(14)).getMillis());
            events.add(event);
            expectedEvents.put(_remoteChannel, event.getData());
        }

        _defaultFanout.copyEvents(events);

        // Events should have been copied in parallel
        verify(_fanoutThreads, times(2)).submit(any(Callable.class));
        assertEquals(_eventsSinked, expectedEvents);

        // Move time forward one more minute
        when(_clock.millis()).thenReturn((now = now.plus(Duration.standardMinutes(1))).getMillis());

        // Add one more event, this one just at the 5 minute threshold for disabling parallelization
        event = newEventAtTime("id0", "fanout-table", "key0", now.minus(Duration.standardMinutes(5)).getMillis());
        _defaultFanout.copyEvents(ImmutableList.of(event));

        // Move time forward one more minute
        when(_clock.millis()).thenReturn((now = now.plus(Duration.standardMinutes(1))).getMillis());

        // Finally add 100 more events and verify they were processed serially
        events.clear();
        _eventsSinked.clear();
        expectedEvents.clear();
        for (int i=1001; i <= 2000; i++) {
            event = newEventAtTime("id" + i, "fanout-table", "key" + i, now.minus(Duration.standardMinutes(5)).getMillis());
            events.add(event);
            expectedEvents.put(_remoteChannel, event.getData());
        }

        reset(_fanoutThreads);
        when(_fanoutThreads.submit(any(Callable.class))).thenAnswer(ignore -> { fail("Fanout should be serial"); return null; });

        _defaultFanout.copyEvents(events);

        // Events should have been copied in serially
        verify(_fanoutThreads, never()).submit(any(Callable.class));
        assertEquals(_eventsSinked, expectedEvents);
    }

    private void addTable(String tableName) {
        Table table = mock(Table.class);
        when(table.getName()).thenReturn(tableName);
        when(table.getAttributes()).thenReturn(ImmutableMap.<String, Object>of());
        when(table.getOptions()).thenReturn(new TableOptionsBuilder().setPlacement("placement").build());
        // Put in another data center to force replication
        when(table.getDataCenters()).thenReturn(ImmutableList.of(_currentDataCenter, _remoteDataCenter));
        when(_dataProvider.getTable(tableName)).thenReturn(table);
    }

    private EventData newEvent(String id, String table, String key) {
        return newEventAtTime(id, table, key, _clock.millis());
    }

    private EventData newEventAtTime(String id, String table, String key, long ts) {
        EventData eventData = mock(EventData.class);
        when(eventData.getId()).thenReturn(id);

        UpdateRef updateRef = new UpdateRef(table, key, TimeUUIDs.uuidForTimeMillis(ts), ImmutableSet.<String>of());
        ByteBuffer data = UpdateRefSerializer.toByteBuffer(updateRef);
        when(eventData.getData()).thenReturn(data);

        return eventData;
    }
}
