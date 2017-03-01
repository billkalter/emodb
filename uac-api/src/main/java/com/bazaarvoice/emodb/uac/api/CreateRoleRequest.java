package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class CreateRoleRequest {
    private String _name;
    private String _description;
    private Set<String> _permissions = ImmutableSet.of();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getName() {
        return _name;
    }

    public CreateRoleRequest setName(String name) {
        _name = name;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDescription() {
        return _description;
    }

    public CreateRoleRequest setDescription(String description) {
        _description = description;
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<String> getPermissions() {
        return _permissions;
    }

    public CreateRoleRequest setPermissions(Set<String> permissions) {
        _permissions = Objects.firstNonNull(permissions, ImmutableSet.of());
        return this;
    }
}
