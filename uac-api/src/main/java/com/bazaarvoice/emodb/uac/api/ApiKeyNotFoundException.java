package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class ApiKeyNotFoundException extends RuntimeException {
    public ApiKeyNotFoundException() {
        super("API Key not found");
    }
}
