package com.bazaarvoice.emodb.auth.identity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class IdentityNotFoundException extends RuntimeException {

    public IdentityNotFoundException() {
        super("Identity not found");
    }
}
