package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Objects;

/**
 * Exception thrown when attempting to create a role which already exists.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class RoleExistsException extends RuntimeException {
    private final String _group;
    private final String _id;

    public RoleExistsException(String group, String id) {
        super("Role exists");
        _group = Objects.firstNonNull(group, RoleIdentifier.NO_GROUP);
        _id = id;
    }

    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }
}