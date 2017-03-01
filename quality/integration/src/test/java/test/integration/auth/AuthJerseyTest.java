package test.integration.auth;

import com.bazaarvoice.emodb.auth.apikey.ApiKey;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityManager;
import com.bazaarvoice.emodb.auth.identity.InMemoryAuthIdentityManager;
import com.bazaarvoice.emodb.auth.permissions.InMemoryPermissionManager;
import com.bazaarvoice.emodb.auth.role.InMemoryRoleManager;
import com.bazaarvoice.emodb.auth.role.Role;
import com.bazaarvoice.emodb.auth.role.RoleManager;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.test.ResourceTest;
import com.bazaarvoice.emodb.web.auth.EmoPermissionResolver;
import com.bazaarvoice.emodb.web.resources.uac.ApiKeyResource1;
import com.bazaarvoice.emodb.web.resources.uac.UserAccessControlResource1;
import com.bazaarvoice.emodb.web.resources.uac.RoleResource1;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.net.HostAndPort;
import com.sun.jersey.api.client.GenericType;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuthJerseyTest extends ResourceTest {

    private final static String APIKEY_ALL_ROLES = "all-roles-key";

    private RoleManager _roleManager = mock(RoleManager.class);
    private AuthIdentityManager<ApiKey> _authIdentityManager;

    @Rule
    public ResourceTestRule _resourceTestRule = setupResourceTestRule();

    private ResourceTestRule setupResourceTestRule() {
        _authIdentityManager = new InMemoryAuthIdentityManager<>();
        _authIdentityManager.createIdentity(APIKEY_ALL_ROLES, new ApiKey("id0", ImmutableSet.of("all-roles")));

        final EmoPermissionResolver permissionResolver = new EmoPermissionResolver(mock(DataStore.class), mock(BlobStore.class));
        final InMemoryPermissionManager permissionManager = new InMemoryPermissionManager(permissionResolver);
        final RoleManager roleManager = new InMemoryRoleManager(permissionManager);

        createRole(roleManager, null, "all-roles", ImmutableSet.of("role|*"));

        return setupResourceTestRule(
                Collections.<Object>singletonList(new UserAccessControlResource1(
                        new RoleResource1(_roleManager),
                        new ApiKeyResource1(_authIdentityManager, HostAndPort.fromParts("localhost", 80), ImmutableSet.of("reserved")))),
                _authIdentityManager,
                permissionManager);
    }

    @After
    public void tearDownMocksAndClearState() {
        verifyNoMoreInteractions(_roleManager);
        reset(_roleManager);
    }

    @Test
    public void testListRoles() {
        when(_roleManager.getAll()).thenReturn(Iterators.emptyIterator());

        // TODO:  Need to provide a proper Jersey client
        List<Role> roles = _resourceTestRule.client().resource(URI.create("/uac/1/role/"))
                .queryParam("APIKey", APIKEY_ALL_ROLES)
                .get(new GenericType<List<Role>>(){});

        assertEquals(ImmutableList.of(), roles);
        verify(_roleManager).getAll();
    }
}
