package com.bazaarvoice.emodb.databus.api;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReplayRequest {

    private final String _subscription;
    private Date _since;
    private URI _s3LogUri;

    public ReplayRequest(String subscription) {
        _subscription = checkNotNull(subscription, "subscription");
    }

    public ReplayRequest since(Date since) {
        _since = since;
        return this;
    }

    public ReplayRequest uploadLogsToS3(String bucket, String path) {
        _s3LogUri = UriBuilder.fromPath(checkNotNull(path, "path"))
                .scheme("s3")
                .host(checkNotNull(bucket, "bucket"))
                .build();
        return this;
    }

    public String getSubscription() {
        return _subscription;
    }

    @Nullable
    public Date getSince() {
        return _since;
    }

    @Nullable
    public URI getS3LogUri() {
        return _s3LogUri;
    }
}
