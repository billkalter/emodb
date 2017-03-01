package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateApiKeyResponse {

    private final String _key;
    private final String _id;

    @JsonCreator
    public CreateApiKeyResponse(@JsonProperty("key") String key, @JsonProperty("id") String id) {
        _key = checkNotNull(key, "key");
        _id = checkNotNull(id, "id");
    }

    public String getKey() {
        return _key;
    }

    public String getId() {
        return _id;
    }
}
