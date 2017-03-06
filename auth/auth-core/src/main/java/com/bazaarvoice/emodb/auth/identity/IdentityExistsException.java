package com.bazaarvoice.emodb.auth.identity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class IdentityExistsException extends RuntimeException {

    public enum Conflict {
        authentication_id,
        internal_id
    }

    private final Conflict _conflict;

    public IdentityExistsException(Conflict conflict) {
        super("Identity exists");
        _conflict = conflict;
    }

    public Conflict getConflict() {
        return _conflict;
    }
}
