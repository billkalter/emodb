package com.bazaarvoice.emodb.web.resources.uac;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.auth.role.RoleModification;
import com.bazaarvoice.emodb.uac.api.CreateRoleRequest;
import com.bazaarvoice.emodb.uac.api.Role;
import com.bazaarvoice.emodb.uac.api.RoleExistsException;
import com.bazaarvoice.emodb.uac.api.RoleIdentifier;
import com.bazaarvoice.emodb.uac.api.RoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.UpdateRoleRequest;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Role: ", description = "All role management operations")
@RequiresAuthentication
public class RoleResource1 {

    private final RoleManager _roleManager;

    @Inject
    public RoleResource1(RoleManager roleManager) {
        _roleManager = roleManager;
    }

    /**
     * Returns all roles for which the caller has read access.  Since the number of roles is typically low
     * this call does not support "from" or "limit" parameters similar to the system or record.
     */
    @GET
    public Iterator<Role> getAllRoles(final @Authenticated Subject subject) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(_roleManager.getAll(), 0), false)
                .filter(role -> subject.hasPermission(Permissions.readRole(role.getRoleIdentifier())))
                .map(this::toUACRole)
                .iterator();
    }

    /**
     * Returns all roles in the specified group for which the caller has read access.  Since the number of roles is
     * typically low this call does not support "from" or "limit" parameters similar to the system or record.
     */
    @GET
    @Path("{group}")
    public Iterator<Role> getAllRolesInGroup(@PathParam("group") GroupParam group,
                                             final @Authenticated Subject subject) {
        return _roleManager.getRolesByGroup(group.get()).stream()
                .filter(role -> subject.hasPermission(Permissions.readRole(role.getRoleIdentifier())))
                .map(this::toUACRole)
                .iterator();
    }

    /**
     * RESTful endpoint for viewing a role.
     */
    @GET
    @Path("{group}/{id}")
    @RequiresPermissions("role|read|{group}|{id}")
    public Role getRole(@PathParam("group") GroupParam group, @PathParam("id") String id) {
        Role role = toUACRole(_roleManager.getRole(toManagerId(group, id)));
        if (role == null) {
            throw new RoleNotFoundException(group.get(), id);
        }
        return role;
    }

    /**
     * RESTful endpoint for creating a role.
     */
    @PUT
    @Path("{group}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("role|create|{group}|{id}")
    public SuccessResponse createRole(@PathParam("group") GroupParam group, @PathParam("id") String id,
                                      Role role) {
        checkArgument(role.getId().equals(new RoleIdentifier(group.get(), id)),
                "Body contains conflicting role identifier");
        return createRoleFromUpdateRequest(group, id, new CreateRoleRequest()
                .setName(role.getName())
                .setDescription(role.getDescription())
                .setPermissions(role.getPermissions()));
    }

    /**
     * RESTful endpoint for updating a role.  Note that all attributes of the role will be updated to match the
     * provided object.
     */
    @POST
    @Path("{group}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("role|update|{group}|{id}")
    public SuccessResponse updateRole(@PathParam("group") GroupParam group, @PathParam("id") String id,
                                      Role role) {
        checkArgument(role.getId().equals(new RoleIdentifier(group.get(), id)),
                "Body contains conflicting role identifier");

        return updateRoleFromUpdateRequest(group, id, new UpdateRoleRequest()
                .setName(role.getName())
                .setDescription(role.getDescription())
                .setGrantedPermissions(role.getPermissions())
                .setRevokeOtherPermissions(true));
    }

    /**
     * RESTful endpoint for deleting a role.
     */
    @DELETE
    @Path("{group}/{id}")
    @RequiresPermissions("role|delete|{group}|{id}")
    public SuccessResponse deleteRole(@PathParam("group") GroupParam group, @PathParam("id") String id) {
        // Backend doesn't care if the role exists or not, but could be confusing to the caller if delete returns
        // success for a role that doesn't exist.  Therefore check if it exists first.
        if (_roleManager.getRole(toManagerId(group, id)) == null) {
            throw new RoleNotFoundException(group.get(), id);
        }

        _roleManager.deleteRole(toManagerId(group, id));
        return SuccessResponse.instance();
    }

    // The following endpoints aren't RESTful like the preceding ones are but are more flexible in allowing partial
    // updates to the role and its permissions, as well as providing the ability to create or update a role and
    // its permissions in a single API call.

    @PUT
    @Path("{group}/{id}")
    @Consumes("application/x.create-role-request")
    @RequiresPermissions("role|create|{group}|{id}")
    public SuccessResponse createRoleFromUpdateRequest(@PathParam("group") GroupParam group, @PathParam("id") String id,
                                                       CreateRoleRequest request) {

        // TODO:  Once supported verify the caller has permission to create each permission granted for this role.

        RoleModification roleModification = new RoleModification()
                .setName(request.getName())
                .setDescription(request.getDescription())
                .setPermissionUpdate(new PermissionUpdateRequest()
                        .permit(request.getPermissions())
                        .revokeRest());
        try {
            _roleManager.createRole(toManagerId(group, id), roleModification);
        } catch (com.bazaarvoice.emodb.auth.role.RoleExistsException e) {
            // Convert to API exception
            throw new RoleExistsException(e.getGroup(), e.getId());
        }

        return SuccessResponse.instance();
    }

    @POST
    @Path("{group}/{id}")
    @Consumes("application/x.update-role-request")
    @RequiresPermissions("role|update|{group}|{id}")
    public SuccessResponse updateRoleFromUpdateRequest(@PathParam("group") GroupParam group, @PathParam("id") String id,
                                                       UpdateRoleRequest request) {

        RoleModification roleModification = new RoleModification();
        if (request.isNamePresent()) {
            roleModification.setName(request.getName());
        }
        if (request.isDescriptionPresent()) {
            roleModification.setDescription(request.getDescription());
        }

        // TODO:  Once supported verify the caller has permission to create each permission granted for this role.

        PermissionUpdateRequest permissionUpdateRequest = new PermissionUpdateRequest();
        if (!request.getGrantedPermissions().isEmpty()) {
            permissionUpdateRequest.permit(request.getGrantedPermissions());
        }
        if (!request.getRevokedPermissions().isEmpty()) {
            permissionUpdateRequest.revoke(request.getRevokedPermissions());
        }
        if (request.isRevokeOtherPermissions()) {
            permissionUpdateRequest.revokeRest();
        }
        roleModification.setPermissionUpdate(permissionUpdateRequest);

        try {
            _roleManager.updateRole(toManagerId(group, id), roleModification);
        } catch (com.bazaarvoice.emodb.auth.role.RoleNotFoundException e) {
            // Convert to API exception
            throw new RoleNotFoundException(e.getGroup(), e.getId());
        }

        return SuccessResponse.instance();
    }

    private Role toUACRole(com.bazaarvoice.emodb.auth.role.Role role) {
        if (role == null) {
            return null;
        }
        Role apiRole = new Role(new RoleIdentifier(role.getGroup(), role.getId()));
        apiRole.setName(role.getName());
        apiRole.setDescription(role.getDescription());

        // Internally roles and permissions are managed separately, but from the UAC API's view permissions are an
        // attribute of the role.  Therefore we have to double-dip back to the role manager to get the permissions.
        apiRole.setPermissions(_roleManager.getPermissionsForRole(role.getRoleIdentifier()));
        
        return apiRole;
    }

    private com.bazaarvoice.emodb.auth.role.RoleIdentifier toManagerId(GroupParam groupParam, String id) {
        return new com.bazaarvoice.emodb.auth.role.RoleIdentifier(groupParam.get(), id);
    }
}
