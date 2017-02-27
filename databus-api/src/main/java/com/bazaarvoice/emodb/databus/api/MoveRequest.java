package com.bazaarvoice.emodb.databus.api;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class MoveRequest {

    private final String _from;
    private final String _to;
    private URI _s3LogUri;

    public static MoveRequestBuilder from(String from) {
        return new MoveRequestBuilder(from);
    }
    
    public static class MoveRequestBuilder {
        private String _from;

        private MoveRequestBuilder(String from) {
            _from = checkNotNull(from, "from");
        }

        public MoveRequest to(String to) {
            return new MoveRequest(_from, checkNotNull(to, "to"));
        }
    }

    private MoveRequest(String from, String to) {
        _from = from;
        _to = to;
    }

    public MoveRequest uploadLogsToS3(String bucket, String path) {
        _s3LogUri = UriBuilder.fromPath(checkNotNull(path, "path"))
                .scheme("s3")
                .host(checkNotNull(bucket, "bucket"))
                .build();
        return this;
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
