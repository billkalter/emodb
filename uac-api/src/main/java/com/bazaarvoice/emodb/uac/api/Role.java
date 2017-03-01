package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Role {

    private final RoleIdentifier _id;
    private String _name;
    private String _description;
    private Set<String> _permissions = ImmutableSet.of();

    @JsonCreator
    private Role(@JsonProperty("group") String group, @JsonProperty("id") String id) {
        this(new RoleIdentifier(
                Objects.firstNonNull(group, RoleIdentifier.NO_GROUP),
                checkNotNull(id, "id")));
    }

    public Role(RoleIdentifier id) {
        _id = checkNotNull(id, "id");
    }

    @JsonProperty("group")
    private String getJsonGroup() {
        return _id.getGroup();
    }

    @JsonProperty("id")
    private String getJsonId() {
        return _id.getId();
    }

    @JsonIgnore
    public RoleIdentifier getId() {
        return _id;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public String getDescription() {
        return _description;
    }

    public void setDescription(String description) {
        _description = description;
    }

    public Set<String> getPermissions() {
        return _permissions;
    }

    public void setPermissions(Set<String> permissions) {
        _permissions = permissions;
    }
}
