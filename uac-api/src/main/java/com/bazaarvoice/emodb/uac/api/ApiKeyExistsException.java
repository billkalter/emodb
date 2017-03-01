package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class ApiKeyExistsException extends RuntimeException {
    public ApiKeyExistsException() {
        super("API Key exists");
    }
}
