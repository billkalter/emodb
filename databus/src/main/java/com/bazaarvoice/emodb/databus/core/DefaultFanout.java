package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Copies events from the "__system_bus:master" event channel or an inbound replication channel to the individual
 * subscription event channels.
 * <p>
 * Each source channel is handled by a single process in the EmoDB cluster.  Generally this copy process is fast enough
 * (and I/O bound) that it's not necessary to spread the work across different servers, but if that changes we can
 * spread writes across multiple source channels (eg. __system_bus:master1, __system_bus:master2, etc.).
 */
public class DefaultFanout extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(DefaultFanout.class);

    private static final int FLUSH_EVENTS_THRESHOLD = 500;
    private static final int FANOUT_THREAD_COUNT = 2;
    private static final int MIN_EVENTS_FOR_PARALLEL_FANOUT = FANOUT_THREAD_COUNT * 20;
    private static final int START_PARALLEL_FANOUT_THRESHOLD_SECONDS = (int) TimeUnit.MINUTES.toSeconds(15);
    private static final int STOP_PARALLEL_FANOUT_THRESHOLD_SECONDS = (int) TimeUnit.MINUTES.toSeconds(5);

    private final String _name;
    private final EventSource _eventSource;
    private final Function<Multimap<String, ByteBuffer>, Void> _eventSink;
    private final boolean _replicateOutbound;
    private final Duration _sleepWhenIdle;
    private final Supplier<Iterable<OwnedSubscription>> _subscriptionsSupplier;
    private final DataCenter _currentDataCenter;
    private final RateLimitedLog _rateLimitedLog;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final Meter _eventsRead;
    private final Meter _eventsWrittenLocal;
    private final Meter _eventsWrittenOutboundReplication;
    private final Clock _clock;
    private final MetricsGroup _lag;
    private final Stopwatch _lastLagStopwatch;
    private final ExecutorService _fanoutThreads;
    private boolean _useParallelFanout = false;
    private int _lastLagSeconds = -1;

    public DefaultFanout(String name,
                         EventSource eventSource,
                         Function<Multimap<String, ByteBuffer>, Void> eventSink,
                         boolean replicateOutbound,
                         Duration sleepWhenIdle,
                         Supplier<Iterable<OwnedSubscription>> subscriptionsSupplier,
                         DataCenter currentDataCenter,
                         RateLimitedLogFactory logFactory,
                         SubscriptionEvaluator subscriptionEvaluator,
                         ExecutorService fanoutThreads,
                         MetricRegistry metricRegistry, Clock clock) {
        _name = checkNotNull(name, "name");
        _eventSource = checkNotNull(eventSource, "eventSource");
        _eventSink = checkNotNull(eventSink, "eventSink");
        _replicateOutbound = replicateOutbound;
        _sleepWhenIdle = checkNotNull(sleepWhenIdle, "sleepWhenIdle");
        _subscriptionsSupplier = checkNotNull(subscriptionsSupplier, "subscriptionsSupplier");
        _currentDataCenter = checkNotNull(currentDataCenter, "currentDataCenter");
        _subscriptionEvaluator = checkNotNull(subscriptionEvaluator, "subscriptionEvaluator");
        _fanoutThreads = checkNotNull(fanoutThreads, "fanoutThreads");

        _rateLimitedLog = logFactory.from(_log);
        _eventsRead = newEventMeter("read", metricRegistry);
        _eventsWrittenLocal = newEventMeter("written-local", metricRegistry);
        _eventsWrittenOutboundReplication = newEventMeter("written-outbound-replication", metricRegistry);
        _lag = new MetricsGroup(metricRegistry);
        _lastLagStopwatch = Stopwatch.createStarted(ClockTicker.getTicker(clock));
        _clock = clock;


        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    private Meter newEventMeter(String name, MetricRegistry metricRegistry) {
        return metricRegistry.meter(metricName(name));
    }
    
    private String metricName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", name, _name);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, _sleepWhenIdle.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
    }

    @Override
    protected void runOneIteration() {
        try {
            //noinspection StatementWithEmptyBody
            while (isRunning() && copyEvents()) {
                // Loop w/o sleeping as long as we keep finding events.
            }
        } catch (Throwable t) {
            // Fanout runs in a continuous loop.  If we get into a bad state, use the rate limited log to avoid
            // flooding the logs with a continuous stream of error messages.  Include the event source name in the
            // message template so we rate limit each event source independently.
            _rateLimitedLog.error(t, "Unexpected fanout exception copying from " + _name + ": {}", t);
            stop();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    @Override
    protected void shutDown() throws Exception {
        // Leadership lost, stop posting fanout lag
        _lag.close();
    }

    private boolean copyEvents() {
        // Use peek() not poll() since LeaderSelector ensures we're not competing with other processes for claims.
        List<EventData> rawEvents = _eventSource.get(1000);

        // If no events, sleep a little while before doing any more work to allow new events to arrive.
        if (rawEvents.isEmpty()) {
            // Update the lag metrics to indicate there is no lag
            updateLagMetrics(null);
            return false;
        }

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        return isRunning() && copyEvents(rawEvents);
    }

    @VisibleForTesting
    boolean copyEvents(List<EventData> rawEvents) {
        // Under normal circumstances a single-threaded fanout is sufficient to keep up with the write rate.  However,
        // when there are high bursts of writes it's possible that the fanout process can start to lag.  When this
        // happens use multiple threads to run the fanout in parallel.  We don't run in parallel all the time because
        // when the lag is low we don't want to add unnecessary stress to the system.

        if (!_useParallelFanout) {
            // If we are not currently using parallel fanout and the lag is currently greater than the threshold then
            // start using parallel fanout.
            if (_lastLagSeconds >= START_PARALLEL_FANOUT_THRESHOLD_SECONDS) {
                _useParallelFanout = true;
            }
        } else if (_lastLagSeconds <= STOP_PARALLEL_FANOUT_THRESHOLD_SECONDS) {
            // If we are not currently using parallel fanout and the lag has dropped belowthe threshold then
            // stop using parallel fanout.
            _useParallelFanout = false;
        }

        Date newestEventTime;

        if (!_useParallelFanout || rawEvents.size() < MIN_EVENTS_FOR_PARALLEL_FANOUT) {
            newestEventTime = copyEventsSync(rawEvents);
        } else {
            List<List<EventData>> partitionedEvents = Lists.partition(rawEvents, rawEvents.size() / (FANOUT_THREAD_COUNT + 1) + 1);
            List<Future<Date>> futures = Lists.newArrayListWithCapacity(FANOUT_THREAD_COUNT);

            // Process all but the last partition asynchronously
            for (final List<EventData> partition : partitionedEvents.subList(0, partitionedEvents.size()-1)) {
                futures.add(_fanoutThreads.submit(() -> copyEventsSync(partition)));
            }
            // Process the last partition synchronously
            newestEventTime = copyEventsSync(partitionedEvents.get(partitionedEvents.size()-1));
            // Wait for the asynchronous fanout threads to complete
            for (Future<Date> future : futures) {
                Futures.getUnchecked(future);
            }
        }

        // Update the lag metrics based on the last event returned.  This isn't perfect for several reasons:
        // 1. In-order delivery is not guaranteed
        // 2. The event time is based on the change ID which is close-to but not precisely the time the update occurred
        // 3. Injected events have artificial change IDs which don't correspond to any clock-based time
        // However, this is still a useful metric because:
        // 1. Delivery is in-order the majority of the time
        // 2. Change IDs are typically within milliseconds of update times
        // 3. Injected events are extremely rare and should be avoided outside of testing anyway
        // 4. The lag only becomes a concern on the scale of minutes, far above the uncertainty introduced by the above
        if (newestEventTime != null) {
            updateLagMetrics(newestEventTime);
        }

        return true;
    }

    private Date copyEventsSync(List<EventData> rawEvents) {
        // Read the list of subscriptions *after* reading events from the event store to avoid race conditions with
        // creating a new subscription.
        Iterable<OwnedSubscription> subscriptions = _subscriptionsSupplier.get();

        // Copy the events to all the destination channels.
        List<String> eventKeys = Lists.newArrayListWithCapacity(rawEvents.size());
        ListMultimap<String, ByteBuffer> eventsByChannel = ArrayListMultimap.create();
        SubscriptionEvaluator.MatchEventData lastMatchEventData = null;
        int numOutboundReplicationEvents = 0;
        for (EventData rawEvent : rawEvents) {
            eventKeys.add(rawEvent.getId());

            ByteBuffer eventData = rawEvent.getData();

            SubscriptionEvaluator.MatchEventData matchEventData;
            try {
                matchEventData = _subscriptionEvaluator.getMatchEventData(eventData);
            } catch (UnknownTableException e) {
                continue;
            }

            // Copy to subscriptions in the current data center.
            for (OwnedSubscription subscription : _subscriptionEvaluator.matches(subscriptions, matchEventData)) {
                eventsByChannel.put(subscription.getName(), eventData);
            }

            // Copy to queues for eventual delivery to remote data centers.
            if (_replicateOutbound) {
                for (DataCenter dataCenter : matchEventData.getTable().getDataCenters()) {
                    if (!dataCenter.equals(_currentDataCenter)) {
                        String channel = ChannelNames.getReplicationFanoutChannel(dataCenter);
                        eventsByChannel.put(channel, eventData);
                        numOutboundReplicationEvents++;
                    }
                }
            }

            // Flush to cap the amount of memory used to buffer events.
            if (eventsByChannel.size() >= FLUSH_EVENTS_THRESHOLD) {
                flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);
                numOutboundReplicationEvents = 0;
            }

            // Track the final match event data record returned
            lastMatchEventData = matchEventData;
        }

        // Final flush.
        flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);

        if (lastMatchEventData != null) {
           return lastMatchEventData.getEventTime();
        }

        return null;
    }

    private void updateLagMetrics(@Nullable Date eventTime) {
        int lagSeconds = eventTime == null ? 0 : (int) TimeUnit.MILLISECONDS.toSeconds(_clock.millis() - eventTime.getTime());
        // As a performance savings only update the metric if both of the following are true:
        // 1. It has been more than 5 seconds since the last time the metric was updated
        // 2. The lag changed since the last posting

        if (lagSeconds != _lastLagSeconds && _lastLagStopwatch.elapsed(TimeUnit.SECONDS) >= 5) {
            _lag.beginUpdates();
            _lag.gauge(metricName("lagSeconds")).set(lagSeconds);
            _lag.endUpdates();

            _lastLagSeconds = lagSeconds;
            _lastLagStopwatch.reset().start();
        }
    }

    private void flush(List<String> eventKeys, Multimap<String, ByteBuffer> eventsByChannel,
                       int numOutboundReplicationEvents) {
        if (!eventsByChannel.isEmpty()) {
            _eventSink.apply(eventsByChannel);
            _eventsWrittenLocal.mark(eventsByChannel.size() - numOutboundReplicationEvents);
            _eventsWrittenOutboundReplication.mark(numOutboundReplicationEvents);
            eventsByChannel.clear();
        }
        if (!eventKeys.isEmpty()) {
            _eventSource.delete(eventKeys);
            _eventsRead.mark(eventKeys.size());
            eventKeys.clear();
        }
    }
}
