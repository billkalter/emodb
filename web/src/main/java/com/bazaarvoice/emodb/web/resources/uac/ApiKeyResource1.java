package com.bazaarvoice.emodb.web.resources.uac;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.uac.api.ApiKeyExistsException;
import com.bazaarvoice.emodb.uac.api.ApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.CreateApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.UpdateApiKeyRequest;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.ReservedRoles;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Produces(MediaType.APPLICATION_JSON)
@Api(value = "API Key: ", description = "All API key management operations")
@RequiresAuthentication
public class ApiKeyResource1 {

    private final AuthIdentityManager<ApiKey> _authIdentityManager;
    private final HostAndPort _hostAndPort;
    private final Set<String> _reservedRoles;

    @Inject
    public ApiKeyResource1(AuthIdentityManager<ApiKey> authIdentityManager,
                           @SelfHostAndPort HostAndPort selfHostAndPort,
                           @ReservedRoles Set<String> reservedRoles) {
        _authIdentityManager = authIdentityManager;
        _hostAndPort = selfHostAndPort;
        _reservedRoles = reservedRoles;
    }

    @GET
    @Path("{id}")
    @RequiresPermissions("apikey|read")
    public com.bazaarvoice.emodb.uac.api.ApiKey getApiKey(@PathParam("id") String id) {
        ApiKey apiKey = _authIdentityManager.getIdentity(id);
        if (apiKey == null) {
            throw new ApiKeyNotFoundException();
        }
        return toUACApiKey(apiKey);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("apikey|create")
    public CreateApiKeyResponse createApiKey(CreateApiKeyRequest request, @QueryParam("key") String key,
                                             @Authenticated Subject subject) {
        // Typically the system creates a random API key for the caller, so the caller has no a-priori knowledge of what
        // the API key will be.  However, for integration tests it is often helpful to create an exact key provided by
        // the caller.  This creates a higher security risk and therefore requires a distinct permission from creating
        // random keys.

        if (key != null && !subject.hasPermission(Permissions.createExactApiKey())) {
            throw new UnauthorizedException();
        }

        Set<RoleIdentifier> roles = toManagerRoleIdentifiers(request.getRoles());
        Set<String> roleStrings = toRoleStrings(roles);

        checkArgument(Sets.intersection(roleStrings, _reservedRoles).isEmpty(), "Cannot assign reserved role");

        // Verify the caller has permission to grant each role
        verifySubjectCanGrantRoles(subject, roles);

        // Generate a unique ID
        String id = createUniqueId();

        String owner = request.getOwner();
        String description = request.getDescription();

        if (key != null) {
            checkArgument(isProvidedApiKeyValid(key), "Provided key is invalid");
            createApiKeyIfAvailable(key, id, owner, description, roleStrings);
        } else {
            key = createRandomApiKey(id, owner, description, roleStrings);
        }

        return new CreateApiKeyResponse(key, id);
    }

    private boolean createApiKeyIfAvailable(String key, String id, String owner, String description, Set<String> roles) {
        boolean exists = _authIdentityManager.getIdentity(key) != null;

        if (exists) {
            return false;
        }

        ApiKey apiKey = new ApiKey(id, roles);
        apiKey.setOwner(owner);
        apiKey.setDescription(description);
        apiKey.setIssued(new Date());

        _authIdentityManager.createIdentity(key, apiKey);

        return true;
    }

    private String createRandomApiKey(String id, String owner, String description, Set<String> roles) {
        // Since API keys are stored hashed we create them in a loop to ensure we don't grab one that is already picked

        String key = null;
        int attempt = 0;

        while (key == null && ++attempt < 10) {
            key = generateRandomApiKey();
            try {
                createApiKeyIfAvailable(key, id, owner, description, roles);
            } catch (ApiKeyExistsException e) {
                // API keys are randomly generated, so this should be exceptionally rare.  Try again with
                // a new randomly-generated key
                key = null;
            }
        }

        if (key == null) {
            throw new ServiceUnavailableException("Failed to generate unique API key", 1);
        }
        return key;
    }

    private String generateRandomApiKey() {
        // Randomize the API key such that it is practically assured that no two call will create the same API key
        // at the same time.
        SecureRandom random = new SecureRandom();
        random.setSeed(System.currentTimeMillis());
        random.setSeed(Thread.currentThread().getId());
        random.setSeed(_hostAndPort.getHostText().getBytes());
        random.setSeed(_hostAndPort.getPort());

        // Use base64 encoding but keep the keys alphanumeric (we could use base64URL() to make them at least URL-safe
        // but pure alphanumeric keeps validation simple).

        byte[] rawKey = new byte[36];
        String key = "";
        do {
            random.nextBytes(rawKey);
            String chars = BaseEncoding.base64().omitPadding().encode(rawKey).toLowerCase();
            // Eliminate all '+' an '/' characters
            chars = chars.replaceAll("\\+|/", "");
            key += chars;
        } while (key.length() < 48);

        return key.substring(0, 48);
    }

    private boolean isProvidedApiKeyValid(String apiKey) {
        return Pattern.matches("[a-zA-Z0-9]{48}", apiKey);
    }
    
    private String createUniqueId() {
        // This is effectively a TimeUUID but condensed to a slightly smaller String representation.
        UUID uuid = TimeUUIDs.newUUID();
        byte[] b = new byte[16];
        System.arraycopy(Longs.toByteArray(uuid.getMostSignificantBits()), 0, b, 0, 8);
        System.arraycopy(Longs.toByteArray(uuid.getLeastSignificantBits()), 0, b, 8, 8);
        return BaseEncoding.base32().omitPadding().encode(b);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public SuccessResponse updateApiKey(@PathParam("id") String id, com.bazaarvoice.emodb.uac.api.ApiKey apiKey,
                                        @Authenticated Subject subject) {
        checkArgument(apiKey.getId().equals(id), "Body contains conflicting API key identifier");
        return updateApiKey(id,
                new UpdateApiKeyRequest()
                    .setOwner(apiKey.getOwner())
                    .setDescription(apiKey.getDescription())
                    .setAssignedRoles(apiKey.getRoles())
                    .setUnassignRemainingRoles(true),
                subject);
    }

    @POST
    @Consumes("application/x.update-apikey-request")
    @Path("{id}")
    public SuccessResponse updateApiKey(@PathParam("id") String id, UpdateApiKeyRequest request,
                                        @Authenticated Subject subject) {

        ApiKey apiKey = _authIdentityManager.getIdentity(id);

        // There's different permissions on the back end for actually modifying the attributes of an API key verses
        // changing the roles assigned to an API key, so depending on the content of the request different
        // permissions may be checked.

        if ((request.isDescriptionPresent() && !Objects.equals(apiKey.getDescription(), request.getDescription())) ||
                (request.isOwnerPresent() && !Objects.equals(apiKey.getOwner(), request.getOwner()))) {
            // API key attribute has changed, verify permission
            if (!subject.hasPermission(Permissions.updateApiKey())) {
                throw new UnauthorizedException();
            }
        }

        Set<RoleIdentifier> grants = toManagerRoleIdentifiers(request.getAssignedRoles());
        Set<RoleIdentifier> revokes = toManagerRoleIdentifiers(request.getUnassignedRoles());

        verifySubjectCanGrantRoles(subject, grants);
        verifySubjectCanRevokeRoles(subject, revokes);

        if (request.isUnassignRemainingRoles()) {
            // Caller has requested to un-grant all roles not explicitly granted.  Verify caller has permissions.
            verifySubjectCanRevokeRoles(subject,
                    apiKey.getRoles().stream()
                            .map(RoleIdentifier::fromString)
                            .filter(role -> !grants.contains(role))
                            .collect(Collectors.toSet()));
        }

        Set<String> grantStrings = toRoleStrings(grants);
        Set<String> revokeStrings = toRoleStrings(revokes);

        Set<String> roles;
        if (request.isUnassignRemainingRoles()) {
            // Only those roles explicitly granted are associated with the API key
            roles = grantStrings;
        } else {
            roles = Sets.newLinkedHashSet(apiKey.getRoles());
            roles.addAll(grantStrings);
            roles.removeAll(revokeStrings);
        }

        ApiKey updatedApiKey = new ApiKey(id, roles);
        updatedApiKey.setOwner(request.isOwnerPresent() ? request.getOwner() : apiKey.getOwner());
        updatedApiKey.setDescription(request.isDescriptionPresent() ? request.getDescription() : apiKey.getDescription());
        updatedApiKey.setMaskedId(apiKey.getMaskedId());
        updatedApiKey.setIssued(apiKey.getIssued());

        _authIdentityManager.updateIdentity(updatedApiKey);

        return SuccessResponse.instance();
    }

    @POST
    @Path("{id}/migrate")
    @RequiresPermissions("apikey|update")
    public CreateApiKeyResponse migrateApiKey(@PathParam("id") String id, @QueryParam("key") String key,
                                @Authenticated Subject subject) {
        // As when creating keys, the caller needs special permission to migrate to an exact API key

        if (key != null) {
            if (!subject.hasPermission(Permissions.createExactApiKey())) {
                throw new UnauthorizedException();
            }
            checkArgument(isProvidedApiKeyValid(key), "Provided key is invalid");
        } else {
            key = generateRandomApiKey();
        }

        _authIdentityManager.migrateIdentity(id, key);

        // Even though we're technically not creating a new API key the response object is the same
        return new CreateApiKeyResponse(id, key);
    }

    @DELETE
    @Path("{id}")
    @RequiresPermissions("apikey|delete")
    public SuccessResponse deleteApiKey(@PathParam("id") String id) {
        _authIdentityManager.deleteIdentity(id);
        return SuccessResponse.instance();
    }

    // Helper methods for converting between UAC and role manager interfaces.

    private Set<RoleIdentifier> toManagerRoleIdentifiers(Set<com.bazaarvoice.emodb.uac.api.RoleIdentifier> roleIdentifiers) {
        return roleIdentifiers.stream()
                .map(this::toManagerRoleIdentifier)
                .collect(Collectors.toSet());
    }

    private RoleIdentifier toManagerRoleIdentifier(com.bazaarvoice.emodb.uac.api.RoleIdentifier roleIdentifier) {
        // API uses RoleIdentifier.NO_GROUP instead of nulls when a role has no group, so convert to null if needed
        return new RoleIdentifier(
                com.bazaarvoice.emodb.uac.api.RoleIdentifier.NO_GROUP.equals(roleIdentifier.getGroup()) ?
                        null : roleIdentifier.getGroup(),
                roleIdentifier.getId());
    }

    private Set<String> toRoleStrings(Set<RoleIdentifier> roleIdentifiers) {
        return roleIdentifiers.stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    private void verifySubjectCanGrantRoles(Subject subject, Set<RoleIdentifier> roles) {
        verifySubjectHasGrantPermissionForRoles(subject, roles, "grant");
    }

    private void verifySubjectCanRevokeRoles(Subject subject, Set<RoleIdentifier> roles) {
        verifySubjectHasGrantPermissionForRoles(subject, roles, "revoke");
    }

    private void verifySubjectHasGrantPermissionForRoles(Subject subject, Set<RoleIdentifier> roles, String operation) {
        for (RoleIdentifier role : roles) {
            if (!subject.hasPermission(Permissions.grantRole(role))) {
                throw new UnauthorizedException(format("Cannot %s role: %s", operation, role));
            }
        }
    }

    private com.bazaarvoice.emodb.uac.api.ApiKey toUACApiKey(ApiKey apiKey) {
        return new com.bazaarvoice.emodb.uac.api.ApiKey(apiKey.getInternalId(), apiKey.getMaskedId(), apiKey.getIssued())
                .setDescription(apiKey.getDescription())
                .setOwner(apiKey.getOwner())
                .setRoles(apiKey.getRoles().stream().map(this::toUACRoleIdentifier).collect(Collectors.toSet()));
    }

    private com.bazaarvoice.emodb.uac.api.RoleIdentifier toUACRoleIdentifier(String roleIdString) {
        RoleIdentifier roleId = RoleIdentifier.fromString(roleIdString);
        return new com.bazaarvoice.emodb.uac.api.RoleIdentifier(roleId.getGroup(), roleId.getId());
    }
}
