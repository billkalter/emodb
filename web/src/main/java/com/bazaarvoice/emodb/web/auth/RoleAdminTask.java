package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyAuthenticationToken;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.inject.Inject;
import io.dropwizard.servlets.tasks.Task;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Task for managing roles and the permissions associated with them.
 *
 * The following examples demonstrate the various ways to use this task.  In order to actually run this task you must
 * provide an API key which has permission to manage roles (see {@link Permissions#manageRoles()}).  For
 * the purposes of this example the API key "admin-key" is a valid key with this permission.
 *
 * Create or update role
 * =====================
 *
 * EmoDB does not distinguish between creating a new role and updating an existing role.  An API key can be associated with
 * any role name, even one that has not been created.  However, until an administrator explicitly assigns permissions
 * to the role it will only be able to perform actions with implicit permissions, such as reading from a SoR table.
 *
 * The following example adds databus poll permissions for subscriptions starting with "foo_" and removes poll permissions
 * for subscriptions starting with "bar_" for role "sample-role":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=update&APIKey=admin-key&role=sample-role\
 *     &permit=databus|poll|foo_*\
 *     &revoke=databus|poll|bar_*"
 * </code>
 *
 * View role
 * =========
 *
 * The following example displays all permissions granted to "sample-role":
 *
 * <code>
 *     $ $ curl -XPOST "localhost:8081/tasks/role?action=view&APIKey=admin-key&role=sample-role"
 * </code>
 *
 * Check role
 * ==========
 *
 * The following example checks whether "sample-role" has permission to poll databus subscription "subscription1":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=check&APIKey=admin-key&role=sample-role&permission=databus|poll|subscription1"
 * </code>
 *
 * Delete role
 * ===========
 *
 * The following example deletes (or, more accurately, removes all permissions from) "sample-role":
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=delete&APIKey=admin-key&role=sample-role"
 * </code>
 *
 * Find deprecated permissions
 * ===========================
 *
 * The following example finds all conditions in all roles that are deprecated.  This should be used to replace
 * all deprecated permissions with equivalent current versions.  Once this returns no values support for deprecated
 * permissions can be safely removed from code.
 *
 * <code>
 *     $ curl -XPOST "localhost:8081/tasks/role?action=find-deprecated-permissions&APIKey=admin-key"
 * </code>
 */
public class RoleAdminTask extends Task {
    private final Logger _log = LoggerFactory.getLogger(RoleAdminTask.class);

    private enum Action {
        VIEW,
        UPDATE,
        DELETE,
        CHECK,
        FIND_DEPRECATED_PERMISSIONS
    }

    private final SecurityManager _securityManager;
    private final PermissionManager _permissionManager;

    @Inject
    public RoleAdminTask(SecurityManager securityManager, PermissionManager permissionManager, TaskRegistry taskRegistry) {
        super("role");
        _securityManager = checkNotNull(securityManager, "securityManager");
        _permissionManager = checkNotNull(permissionManager, "permissionManager");

        taskRegistry.addTask(this);
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output)
            throws Exception {
        Subject subject = new Subject.Builder(_securityManager).buildSubject();
        try {
            // Make sure the API key is valid
            String apiKey = getValueFromParams(ApiKeyRequest.AUTHENTICATION_PARAM, parameters);
            subject.login(new ApiKeyAuthenticationToken(apiKey));

            // Make sure the API key is permitted to manage roles
            subject.checkPermission(Permissions.manageRoles());

            String activityStr = getValueFromParams("action", parameters);
            Action action = Action.valueOf(activityStr.toUpperCase().replace('-', '_'));

            switch (action) {
                case VIEW:
                    viewRole(getRole(parameters), output);
                    break;
                case UPDATE:
                    updateRole(getRole(parameters), parameters, subject, output);
                    break;
                case DELETE:
                    deleteRole(getRole(parameters), output);
                    break;
                case CHECK:
                    checkPermission(getRole(parameters), parameters, output);
                    break;
                case FIND_DEPRECATED_PERMISSIONS:
                    findDeprecatedPermissions(output);
            }
        } catch (AuthenticationException | AuthorizationException e) {
            _log.warn("Unauthorized attempt to access role management task");
            output.println("Not authorized");
        } finally {
            subject.logout();
        }
    }

    private String getRole(ImmutableMultimap<String, String> parameters) {
        return getValueFromParams("role", parameters);
    }

    private void viewRole(String role, PrintWriter output) {
        List<String> permissions = _permissionManager.getAllForRole(role).stream()
                .map(Object::toString)
                .sorted()
                .collect(Collectors.toList());

        output.println(String.format("%s has %d permissions", role, permissions.size()));
        for (String permission : permissions) {
            output.println("- " + permission);
        }
    }

    private void updateRole(String role, ImmutableMultimap<String, String> parameters, Subject subject, PrintWriter output) {
        checkArgument(!DefaultRoles.isDefaultRole(role), "Cannot update default role: %s", role);

        Set<String> permitSet = ImmutableSet.copyOf(parameters.get("permit"));
        Set<String> revokeSet = ImmutableSet.copyOf(parameters.get("revoke"));

        checkArgument(Sets.intersection(permitSet, revokeSet).isEmpty(),
                "Cannot permit and revoke the same permission in a single request");

        // Verify that all permissions being permitted can be assigned to a role.

        List<String> unassignable = Lists.newArrayList();
        List<String> notPermitted = Lists.newArrayList();

        for (String permit : permitSet) {
            // All permissions returned are EmoPermission instances.
            EmoPermission resolved = (EmoPermission) _permissionManager.getPermissionResolver().resolvePermission(permit);
            if (!resolved.isAssignable()) {
                unassignable.add(permit);
            }
            if (!subject.isPermitted(resolved)) {
                notPermitted.add(permit);
            }
        }

        if (!unassignable.isEmpty()) {
            output.println("The following permission(s) cannot be assigned to a role:");
            for (String permit : unassignable) {
                output.println("- " + permit);
            }
            output.println("Please rewrite the above permission(s) using constants, wildcard strings, or \"if()\" expressions");
            return;
        }

        if (!notPermitted.isEmpty()) {
            output.println("You do not has sufficient permissions to grant the following permission(s):");
            for (String permit : notPermitted) {
                output.println("- " + permit);
            }
            output.println("Please remove or rewrite the above permission(s) constrained within your permissions");
            return;
        }

        _permissionManager.updateForRole(role,
                new PermissionUpdateRequest()
                        .permit(permitSet)
                        .revoke(revokeSet));

        output.println("Role updated.");
        viewRole(role, output);
    }

    private void deleteRole(String role, PrintWriter output) {
        // A role technically cannot be deleted.  What this method does is revoke all permissions associated with
        // the role.

        checkArgument(!DefaultRoles.isDefaultRole(role), "Cannot delete default role: %s", role);

        Set<Permission> permissions = _permissionManager.getAllForRole(role);

        // Bound the number of times we'll try to revoke all permissions.
        for (int attempt = 0; !permissions.isEmpty() && attempt < 10; attempt++) {
            _permissionManager.updateForRole(role,
                    new PermissionUpdateRequest()
                            .revoke(permissions.stream().map(Object::toString).collect(Collectors.toList())));

            permissions = _permissionManager.getAllForRole(role);
        }

        if (permissions.isEmpty()) {
            output.println("Role deleted");
        } else {
            output.println(String.format("WARNING:  Role still had %d permissions after 10 delete attempts", permissions.size()));
        }
    }

    private void checkPermission(String role, ImmutableMultimap<String, String> parameters, PrintWriter output) {
        String permissionStr = getValueFromParams("permission", parameters);
        final Permission permission = _permissionManager.getPermissionResolver().resolvePermission(permissionStr);

        List<String> matchingPermissions = _permissionManager.getAllForRole(role).stream()
                .filter(grantedPermission -> grantedPermission.implies(permission))
                .map(Object::toString)
                .sorted(Ordering.natural())
                .collect(Collectors.toList());

        if (!matchingPermissions.isEmpty()) {
            output.println(String.format("%s is permitted %s by the following:", role, permissionStr));
            for (String matchingPermission : matchingPermissions) {
                output.println("- " + matchingPermission);
            }
        } else {
            output.println(String.format("%s is not permitted %s", role, permissionStr));
        }
    }

    private void findDeprecatedPermissions(PrintWriter output) {
        TreeMultimap<String, String> deprecatedPermissionsByRole =
                StreamSupport.stream(_permissionManager.getAll().spliterator(), false)
                        .flatMap(e -> e.getValue().stream().map(permission -> Maps.immutableEntry(e.getKey(), permission)))
                        .filter(e -> !((EmoPermission) e.getValue()).isAssignable())
                        .collect(TreeMultimap::create,
                                (map, e) -> map.put(e.getKey(), e.getValue().toString()),
                                TreeMultimap::putAll);

        if (deprecatedPermissionsByRole.isEmpty()) {
            output.println("There are no roles with deprecated permissions.");
        } else {
            output.println("The following roles have deprecated permissions:\n");
            for (String role : ImmutableSortedSet.copyOf(deprecatedPermissionsByRole.keySet())) {
                output.println(role);
                for (String permission : deprecatedPermissionsByRole.get(role)) {
                    output.println("- " + permission);
                }
            }
        }
    }

    private String getValueFromParams(String value, ImmutableMultimap<String, String> parameters) {
        try {
            return Iterables.getOnlyElement(parameters.get(value));
        } catch (Exception e) {
            throw new IllegalArgumentException(format("A single '%s' parameter value is required", value));
        }
    }
}
