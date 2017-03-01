package com.bazaarvoice.emodb.uac.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;

import java.util.Iterator;

public interface AuthUserAccessControl {

    Iterator<Role> getAllRoles(@Credential String apiKey);

    Iterator<Role> getAllRolesInGroup(@Credential String apiKey, String group);

    Role getRole(@Credential String apiKey, RoleIdentifier id);

    void createRole(@Credential String apiKey, RoleIdentifier id, CreateRoleRequest request);

    void updateRole(@Credential String apiKey, RoleIdentifier id, UpdateRoleRequest request);

    void deleteRole(@Credential String apiKey, RoleIdentifier id);

    ApiKey getApiKey(@Credential String apiKey, String id);

    CreateApiKeyResponse createApiKey(@Credential String apiKey, CreateApiKeyRequest request);

    void updateApiKey(@Credential String apiKey, String id, UpdateApiKeyRequest request);

    String migrateApiKey(@Credential String apiKey, String id);
    
    void deleteApiKey(@Credential String apiKey, String id);
}
