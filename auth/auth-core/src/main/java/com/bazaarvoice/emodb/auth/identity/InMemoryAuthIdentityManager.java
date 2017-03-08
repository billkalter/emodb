package com.bazaarvoice.emodb.auth.identity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple in-memory implementation of an {@link AuthIdentityManager}.
 */
public class InMemoryAuthIdentityManager<T extends AuthIdentity> implements AuthIdentityManager<T> {

    private final Map<String, String> _authenticationToInternalIdMap = Maps.newConcurrentMap();
    private final Map<String, T> _identityMap = Maps.newConcurrentMap();

    @Override
    synchronized public void createIdentity(String internalId, String authenticationId, AuthIdentityModification<T> modification)
            throws IdentityExistsException {
        if (_authenticationToInternalIdMap.containsKey(authenticationId)) {
            throw new IdentityExistsException(IdentityExistsException.Conflict.authentication_id);
        }
        if (_identityMap.containsKey(internalId)) {
            throw new IdentityExistsException(IdentityExistsException.Conflict.internal_id);
        }
        T identity = modification.buildNew(internalId);
        identity.setIssued(new Date());
        _authenticationToInternalIdMap.put(authenticationId, internalId);
        _identityMap.put(internalId, identity);
    }

    @Override
    public T getIdentityByAuthenticationId(String authenticationId) {
        String id = _authenticationToInternalIdMap.get(authenticationId);
        if (id == null) {
            return null;
        }
        return _identityMap.get(id);
    }

    @Override
    public T getIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        return _identityMap.get(internalId);
    }

    @Override
    synchronized public void updateIdentity(String internalId, AuthIdentityModification<T> modification)
            throws IdentityNotFoundException {
        T existing = _identityMap.get(internalId);
        if (existing == null) {
            throw new IdentityNotFoundException();
        }
        _identityMap.put(internalId, modification.buildFrom(existing));
    }

    @Override
    synchronized public void migrateIdentity(String internalId, String newAuthenticationId) {
        if (_authenticationToInternalIdMap.containsKey(newAuthenticationId)) {
            throw new IdentityExistsException(IdentityExistsException.Conflict.authentication_id);
        }
        if (!_identityMap.containsKey(internalId)) {
            throw new IdentityNotFoundException();
        }
        deleteAuthenticationReferenceToInternalId(internalId);
        _authenticationToInternalIdMap.put(newAuthenticationId, internalId);
    }

    @Override
    synchronized public void deleteIdentity(String internalId) {
        checkNotNull(internalId, "internalId");
        _identityMap.remove(internalId);
        deleteAuthenticationReferenceToInternalId(internalId);
    }

    private void deleteAuthenticationReferenceToInternalId(String internalId) {
        for (Map.Entry<String, String> entry : _authenticationToInternalIdMap.entrySet()) {
            if (entry.getValue().equals(internalId)) {
                _authenticationToInternalIdMap.remove(entry.getKey());
                return;
            }
        }
    }

    public void reset() {
        _identityMap.clear();
    }

    public List<T> getAllIdentities() {
        return ImmutableList.copyOf(_identityMap.values());
    }
}
