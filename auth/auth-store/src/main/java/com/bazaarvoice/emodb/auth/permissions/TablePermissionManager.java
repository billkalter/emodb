package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.InvalidPermissionStringException;
import org.apache.shiro.authz.permission.PermissionResolver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Permission manager that uses an EmoDB table to store permissions.
 *
 * With a level of irony not seen since Alanis Morissette the application must have permission to perform operations
 * on the table in order to use this manager.
 */
public class TablePermissionManager implements PermissionManager {

    private final PermissionResolver _permissionResolver;
    private final DataStore _dataStore;
    private final String _tableName;
    private final String _placement;
    private volatile boolean _tableValidated;

    public TablePermissionManager(PermissionResolver permissionResolver, DataStore dataStore,
                                  String tableName, String placement) {
        _permissionResolver = checkNotNull(permissionResolver, "permissionResolver");
        _dataStore = checkNotNull(dataStore, "dataStore");
        _tableName = checkNotNull(tableName, "tableName");
        _placement = checkNotNull(placement, "placement");
    }

    @Override
    public Set<Permission> getPermissions(String id) {
        checkNotNull(id, "id");
        validateTable();
        Map<String, Object> map = _dataStore.get(_tableName, id, ReadConsistency.STRONG);
        return extractPermissionsFromRecord(map);
    }

    private Set<Permission> extractPermissionsFromRecord(Map<String, Object> map) {
        Set<Permission> permissions = Sets.newHashSet();

        for (String mapKey : map.keySet()) {
            if (mapKey.startsWith("perm_")) {
                permissions.add(_permissionResolver.resolvePermission(mapKey.substring(5)));
            }
        }

        return permissions;
    }

    @Override
    public void updatePermissions(String id, PermissionUpdateRequest request) {
        checkNotNull(id, "id");
        checkNotNull(request, "request");
        validateTable();

        Delta delta = createDelta(request);
        if (delta == null) {
            // Request did not change the permissions at all.  Take no action.
            return;
        }

        _dataStore.update(
                _tableName,
                id,
                TimeUUIDs.newUUID(),
                delta,
                new AuditBuilder().setLocalHost().setComment("update permissions").build(),
                WriteConsistency.GLOBAL);
    }

    /**
     * Returns a delta constructed from this request, or null if the request contained no changes.
     */
    @Nullable
    private Delta createDelta(PermissionUpdateRequest request) {
        MapDeltaBuilder builder = Deltas.mapBuilder();
        boolean modified = false;

        for (String permissionString : request.getPermitted()) {
            builder.put("perm_" + validated(permissionString), 1);
            modified = true;
        }
        for (String permissionString : request.getRevoked()) {
            builder.remove("perm_" + validated(permissionString));
            modified = true;
        }
        if (request.isRevokeRest()) {
            builder.removeRest();
            modified = true;
        }

        if (modified) {
            return builder.build();
        }

        // Request contained no changes
        return null;
    }

    /**
     * Validates that a permission string can be resolved.
     */
    private String validated(String permissionString)
            throws InvalidPermissionStringException {
        _permissionResolver.resolvePermission(permissionString);
        return permissionString;
    }

    @Override
    public void revokePermissions(String id) {
        checkNotNull(id, "id");
        validateTable();

        _dataStore.update(
                _tableName,
                id,
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                new AuditBuilder().setLocalHost().setComment("delete permissions").build(),
                WriteConsistency.GLOBAL);

    }

    @Override
    public Iterable<Map.Entry<String, Set<Permission>>> getAll() {
        validateTable();

        return () -> {
            Iterator<Map<String, Object>> records = _dataStore.scan(_tableName, null, Long.MAX_VALUE, ReadConsistency.STRONG);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(records, 0), false)
                    .map(record -> Maps.immutableEntry(Intrinsic.getId(record), extractPermissionsFromRecord(record)))
                    .iterator();
        };
    }

    @Override
    public PermissionResolver getPermissionResolver() {
        return _permissionResolver;
    }

    private void validateTable() {
        if (_tableValidated) {
            return;
        }

        synchronized(this) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_placement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("create permissions table").build());
            }

            _tableValidated = true;
        }
    }
}
