package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ApiKey {

    private final String _id;
    private final String _maskedKey;
    private final Date _issued;
    private String _owner;
    private String _description;
    private Set<RoleIdentifier> _roles = ImmutableSet.of();

    @JsonCreator
    public ApiKey(@JsonProperty("id") String id, @JsonProperty("maskedKey") String maskedKey,
                  @JsonProperty("issued") Date issued) {
        _id = checkNotNull(id, "id");
        _maskedKey = checkNotNull(maskedKey, "maskedKey");
        _issued = checkNotNull(issued, "issued");
    }

    public String getId() {
        return _id;
    }

    public String getMaskedKey() {
        return _maskedKey;
    }

    public Date getIssued() {
        return _issued;
    }

    public String getOwner() {
        return _owner;
    }

    public ApiKey setOwner(String owner) {
        _owner = owner;
        return this;
    }

    public String getDescription() {
        return _description;
    }

    public ApiKey setDescription(String description) {
        _description = description;
        return this;
    }

    public Set<RoleIdentifier> getRoles() {
        return _roles;
    }

    public ApiKey setRoles(Set<RoleIdentifier> roles) {
        _roles = checkNotNull(roles, "roles");
        return this;
    }
}
