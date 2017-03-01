package com.bazaarvoice.emodb.uac.api;

import java.util.Iterator;

public interface UserAccessControl {

    Iterator<Role> getAllRoles();

    Iterator<Role> getAllRolesInGroup(String group);

    Role getRole(RoleIdentifier id);

    void createRole(RoleIdentifier id, CreateRoleRequest request);

    void updateRole(RoleIdentifier id, UpdateRoleRequest request);

    void deleteRole(RoleIdentifier id);

    ApiKey getApiKey(String id);

    CreateApiKeyResponse createApiKey(CreateApiKeyRequest request);

    void updateApiKey(String id, UpdateApiKeyRequest request);

    String migrateApiKey(String id);
    
    void deleteApiKey(String id);
}
