package com.bazaarvoice.emodb.uac.api;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class CreateApiKeyRequest {

    private String _owner;
    private String _description;
    private Set<RoleIdentifier> _roles = ImmutableSet.of();

    public String getOwner() {
        return _owner;
    }

    public CreateApiKeyRequest setOwner(String owner) {
        _owner = owner;
        return this;
    }

    public String getDescription() {
        return _description;
    }

    public CreateApiKeyRequest setDescription(String description) {
        _description = description;
        return this;
    }

    public Set<RoleIdentifier> getRoles() {
        return _roles;
    }

    public CreateApiKeyRequest setRoles(Set<RoleIdentifier> roles) {
        _roles = Objects.firstNonNull(roles, ImmutableSet.of());
        return this;
    }
}
