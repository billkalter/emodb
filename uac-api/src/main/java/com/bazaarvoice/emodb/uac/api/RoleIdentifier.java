package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class RoleIdentifier {

    public static final String NO_GROUP = "_";

    private final String _group;
    private final String _id;

    public RoleIdentifier(String id) {
        this(NO_GROUP, id);
    }

    @JsonCreator
    public RoleIdentifier(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        _group = Objects.firstNonNull(group, NO_GROUP);
        _id = checkNotNull(id, "id");
    }

    public String getGroup() {
        return _group;
    }

    public String getId() {
        return _id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RoleIdentifier)) {
            return false;
        }

        RoleIdentifier that = (RoleIdentifier) o;

        return _group.equals(that._group) && _id.equals(that._id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_group, _id);
    }
}
