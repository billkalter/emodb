package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRealm;
import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.apikey.ApiKeySecurityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionManager;
import com.bazaarvoice.emodb.auth.permissions.PermissionUpdateRequest;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.web.auth.DefaultRoles;
import com.bazaarvoice.emodb.web.auth.EmoPermission;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.RoleAdminTask;
import com.bazaarvoice.emodb.web.auth.resource.ConditionResource;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class RoleAdminTaskTest {
    private RoleAdminTask _task;
    private InMemoryAuthIdentityManager<ApiKey> _authIdentityManager;
    private InMemoryPermissionManager _permissionManager;

    @BeforeMethod
    public void setUp() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>();
        EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        _permissionManager = new InMemoryPermissionManager(permissionResolver);

        _permissionManager.updateForRole(DefaultRoles.admin.toString(), new PermissionUpdateRequest().permit(Permissions.manageRoles()));
        ApiKeySecurityManager securityManager = new ApiKeySecurityManager(
                new ApiKeyRealm("test", new MemoryConstrainedCacheManager(), _authIdentityManager, _permissionManager,
                        null));

        _task = new RoleAdminTask(securityManager, _permissionManager, mock(TaskRegistry.class));
        _authIdentityManager.updateIdentity(new ApiKey("test-admin", "id_admin", ImmutableSet.of(DefaultRoles.admin.toString())));
    }

    @AfterMethod
    public void tearDown() {
        ThreadContext.remove();
    }

    @Test
    public void testInvalidApiKey()
            throws Exception {
        StringWriter out = new StringWriter();
        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "invalid-key",
                        "action", "view",
                        "role", "any-role"),
                new PrintWriter(out));

        assertEquals(out.toString(), "Not authorized\n");
    }

    @Test
    public void testInsufficientPermissionsToManageRoles()
            throws Exception {
        // Notably this role does not have manageRoles() permission
        _permissionManager.updateForRole("insufficient-role", new PermissionUpdateRequest().permit("sor|read|*"));
        _authIdentityManager.updateIdentity(new ApiKey("insufficient-key", "id0", ImmutableSet.of("insufficient-role")));

        StringWriter out = new StringWriter();
        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "insufficient-key",
                "action", "view",
                "role", "any-role"),
                new PrintWriter(out));

        assertEquals(out.toString(), "Not authorized\n");
    }

    @Test
    public void testViewRole()
            throws Exception {
        _permissionManager.updateForRole("view-role",
                new PermissionUpdateRequest()
                        .permit("queue|post|foo", "queue|poll|foo", "sor|update|test:*", "blob|update|test:*"));

        StringWriter out = new StringWriter();

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "view",
                        "role", "view-role"),
                new PrintWriter(out));

        String[] lines = out.toString().split("\\n");
        assertEquals(lines.length, 5);
        // First line is a header.  Skip this and verify the remaining lines are the permissions alphabetically sorted
        assertEquals(lines[1], "- blob|update|test:*");
        assertEquals(lines[2], "- queue|poll|foo");
        assertEquals(lines[3], "- queue|post|foo");
        assertEquals(lines[4], "- sor|update|test:*");
    }

    @Test
    public void testCreateRole()
            throws Exception {
        // First, admin role needs permission to create the roles it is permitting
        _permissionManager.updateForRole(DefaultRoles.admin.toString(), new PermissionUpdateRequest().permit("*"));

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "update",
                "role", "new-role",
                "permit", "sor|update|if({..,\"foo\":\"bar\"})",
                "permit", "queue|post|*"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        Set<Permission> permissions = _permissionManager.getAllForRole("new-role");
        Set<Permission> expected = toPermissionSet(ImmutableList.of(
                Permissions.updateSorTable(
                        new ConditionResource(Conditions.mapBuilder()
                                .contains("foo", "bar")
                                .build())),
                Permissions.postQueue(Permissions.ALL)));
        assertEquals(permissions, expected);
    }

    @Test
    public void testUpdateRole()
            throws Exception {
        // First, admin role needs permission to create the roles it is permitting
        _permissionManager.updateForRole(DefaultRoles.admin.toString(), new PermissionUpdateRequest().permit("*"));

        _permissionManager.updateForRole("existing-role",
                new PermissionUpdateRequest()
                        .permit("queue|post|foo", "queue|post|bar"));

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "update",
                        "role", "existing-role",
                        "permit", "queue|post|baz",
                        "revoke", "queue|post|bar"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        Set<Permission> permissions = _permissionManager.getAllForRole("existing-role");
        Set<Permission> expected = toPermissionSet(ImmutableList.of(
                Permissions.postQueue(new NamedResource("foo")),
                Permissions.postQueue(new NamedResource("baz"))));
        assertEquals(permissions, expected);
    }

    @Test
    public void testDeleteRole()
            throws Exception {
        _permissionManager.updateForRole("delete-role",
                new PermissionUpdateRequest()
                        .permit("queue|post|foo", "queue|post|bar"));

        _task.execute(ImmutableMultimap.of(
                        ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                        "action", "delete",
                        "role", "delete-role"),
                new PrintWriter(ByteStreams.nullOutputStream()));

        Set<Permission> permissions = _permissionManager.getAllForRole("delete-role");
        assertEquals(permissions, ImmutableList.<Permission>of());
    }

    @Test
    public void testCannotModifyDefaultRole()
            throws Exception {

        for (DefaultRoles defaultRole : DefaultRoles.values()) {
            try {
                _task.execute(ImmutableMultimap.of(
                                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                                "action", "update",
                                "role", defaultRole.toString(),
                                "permit", "sor|update|test:*"),
                        new PrintWriter(ByteStreams.nullOutputStream()));
                 fail("Update role did not fail " + defaultRole);
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), "Cannot update default role: " + defaultRole);
            }

            try {
                _task.execute(ImmutableMultimap.of(
                                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                                "action", "delete",
                                "role", defaultRole.toString()),
                        new PrintWriter(ByteStreams.nullOutputStream()));
                fail("Delete role did not fail " + defaultRole);
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), "Cannot delete default role: " + defaultRole);
            }
        }
    }

    @Test
    public void testCheckRole()
            throws Exception {

        _permissionManager.updateForRole("check-role",
                new PermissionUpdateRequest()
                        .permit("sor|update|c*", "sor|update|ch*"));

        Map<String, List<String>> expected = ImmutableMap.<String, List<String>> of(
                "sor|update|check", ImmutableList.of("- sor|update|c*", "- sor|update|ch*"),
                "sor|update|create", ImmutableList.of("- sor|update|c*"),
                "sor|update|notme", ImmutableList.<String>of());

        for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
            StringWriter out = new StringWriter();

            _task.execute(ImmutableMultimap.of(
                            ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                            "action", "check",
                            "role", "check-role",
                            "permission", entry.getKey()),
                    new PrintWriter(out));

            List<String> actual = ImmutableList.copyOf(out.toString().split("\n"));

            // The first line is a header; skip it and check the remaining lines
            assertEquals(actual.subList(1, actual.size()), entry.getValue());
        }
    }

    @Test
    public void testFindDeprecatedPermissions() throws Exception {
        ApiKeySecurityManager securityManager = mock(ApiKeySecurityManager.class);
        when(securityManager.createSubject(any())).thenReturn(mock(Subject.class));

        EmoPermission deprecated = mock(EmoPermission.class);
        when(deprecated.toString()).thenReturn("deprecated");
        when(deprecated.isAssignable()).thenReturn(false);

        EmoPermission current = mock(EmoPermission.class);
        when(current.toString()).thenReturn("current");
        when(current.isAssignable()).thenReturn(true);

        PermissionManager permissionManager = mock(PermissionManager.class);
        when(permissionManager.getAll()).thenReturn(ImmutableList.<Map.Entry<String, Set<Permission>>>builder()
                .add(Maps.immutableEntry("onlyCurrent", ImmutableSet.of(current)))
                .add(Maps.immutableEntry("onlyDeprecated", ImmutableSet.of(deprecated)))
                .add(Maps.immutableEntry("both", ImmutableSet.of(current, deprecated)))
                .build());

        _task = new RoleAdminTask(securityManager, permissionManager, mock(TaskRegistry.class));

        StringWriter out = new StringWriter();

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "find_deprecated_permissions"),
                new PrintWriter(out));

        List<String> actual = ImmutableList.copyOf(out.toString().split("\n"));
        // First two lines are header
        assertEquals(actual.subList(2, actual.size()), ImmutableList.of("both", "- deprecated", "onlyDeprecated", "- deprecated"));
    }

    @Test
    public void testCreateRoleWithUnboundPermissions() throws Exception  {
        // First, give admin role all permissions within the "sor" context
        _permissionManager.updateForRole(DefaultRoles.admin.toString(), new PermissionUpdateRequest().permit("sor|*"));

        StringWriter out = new StringWriter();

        _task.execute(ImmutableMultimap.of(
                ApiKeyRequest.AUTHENTICATION_PARAM, "test-admin",
                "action", "update",
                "role", "new-role",
                "permit", "sor|update|if({..,\"foo\":\"bar\"})",
                "permit", "queue|post|*"),
                new PrintWriter(out));

        List<String> actual = ImmutableList.copyOf(out.toString().split("\n"));

        assertEquals(actual, ImmutableList.of(
                "You do not has sufficient permissions to grant the following permission(s):",
                "- queue|post|*",
                "Please remove or rewrite the above permission(s) constrained within your permissions"));
    }

    private Set<Permission> toPermissionSet(Iterable<String> permissionStrings) {
        ImmutableSet.Builder<Permission> builder = ImmutableSet.builder();
        for (String permissionString : permissionStrings) {
            builder.add(_permissionManager.getPermissionResolver().resolvePermission(permissionString));
        }
        return builder.build();
    }
}
