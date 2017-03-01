package com.bazaarvoice.emodb.auth.role;

import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Request parameter for updating a role using {@link RoleManager#createRole(RoleIdentifier, RoleModification)}
 * or {@link RoleManager#updateRole(RoleIdentifier, RoleModification)}.
 */
public class RoleModification {

    private String _name;
    private boolean _namePresent = false;
    private String _description;
    private boolean _descriptionPresent = false;
    private PermissionUpdateRequest _permissionUpdate;

    public RoleModification setName(String name) {
        _name = name;
        _namePresent = true;
        return this;
    }

    public RoleModification setDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public RoleModification setPermissionUpdate(PermissionUpdateRequest permissionUpdate) {
        _permissionUpdate = permissionUpdate;
        return this;
    }

    public String getName() {
        return _name;
    }

    @JsonIgnore
    public boolean isNamePresent() {
        return _namePresent;
    }

    public String getDescription() {
        return _description;
    }

    @JsonIgnore
    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public PermissionUpdateRequest getPermissionUpdate() {
        return _permissionUpdate;
    }
}
