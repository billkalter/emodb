package com.bazaarvoice.emodb.databus.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class MoveSubscriptionRequest {

    private final String _ownerId;
    private final String _from;
    private final String _to;
    @Nullable
    private final URI _s3LogUri;

    @JsonCreator
    public MoveSubscriptionRequest(@JsonProperty ("ownerId") String ownerId,
                                   @JsonProperty ("from") String from,
                                   @JsonProperty ("to") String to,
                                   @JsonProperty ("s3LogUri") URI s3LogUri) {
        _ownerId = checkNotNull(ownerId, "ownerId");
        _from = checkNotNull(from, "from");
        _to = checkNotNull(to, "to");
        _s3LogUri = s3LogUri;
    }

    public String getOwnerId() {
        return _ownerId;
    }

    public String getFrom() {
        return _from;
    }

    public String getTo() {
        return _to;
    }

    @Nullable
    public URI getS3LogUri() {
        return _s3LogUri;
    }
}
