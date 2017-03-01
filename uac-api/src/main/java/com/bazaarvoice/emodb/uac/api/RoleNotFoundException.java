package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Objects;

/**
 * Exception thrown when attempting to utilize a role which does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class RoleNotFoundException extends RuntimeException {
    private final String _group;
    private final String _id;

    public RoleNotFoundException(String group, String id) {
        super("Role not found");
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