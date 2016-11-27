package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @see com.bazaarvoice.emodb.databus.api.Databus
 */
public interface SubjectDatabus {
    Iterator<Subscription> listSubscriptions(Subject subject, @Nullable String fromSubscriptionExclusive, long limit);

    void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl);

    void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter);

    void unsubscribe(Subject subject, String subscription);

    Subscription getSubscription(Subject subject, String subscription)
            throws UnknownSubscriptionException;

    long getEventCount(Subject subject, String subscription);

    long getEventCountUpTo(Subject subject, String subscription, long limit);

    long getClaimCount(Subject subject, String subscription);

    List<Event> peek(Subject subject, String subscription, int limit);

    List<Event> poll(Subject subject, String subscription, Duration claimTtl, int limit);

    void renew(Subject subject, String subscription, Collection<String> eventKeys, Duration claimTtl);

    void acknowledge(Subject subject, String subscription, Collection<String> eventKeys);

    String replayAsync(Subject subject, String subscription);

    String replayAsyncSince(Subject subject, String subscription, Date since);

    ReplaySubscriptionStatus getReplayStatus(Subject subject, String reference);

    String moveAsync(Subject subject, String from, String to);

    MoveSubscriptionStatus getMoveStatus(Subject subject, String reference);

    void injectEvent(Subject subject, String subscription, String table, String key);

    void unclaimAll(Subject subject, String subscription);

    void purge(Subject subject, String subscription);
}
