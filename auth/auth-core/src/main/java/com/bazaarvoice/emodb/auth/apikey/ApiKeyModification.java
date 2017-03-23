package com.bazaarvoice.emodb.auth.apikey;

import com.bazaarvoice.emodb.auth.identity.AuthIdentity;
import com.bazaarvoice.emodb.auth.identity.AuthIdentityModification;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link AuthIdentityModification} implementation for API keys.  Since API keys introduce no new attributes to
 * {@link AuthIdentity} this class only defines the two builder methods.
 */
public class ApiKeyModification extends AuthIdentityModification<ApiKey> {

    @Override
    public ApiKey buildNew(String internalId) {
        checkNotNull(internalId, "internalId");
        return buildFrom(new ApiKey(internalId, getUpdatedRolesFrom(ImmutableSet.of())));
    }

    @Override
    public ApiKey buildFrom(ApiKey identity) {
        checkNotNull(identity, "identity");
        ApiKey apiKey = new ApiKey(identity.getInternalId(), getUpdatedRolesFrom(identity.getRoles()));
        apiKey.setOwner(isOwnerPresent() ? getOwner() : identity.getOwner());
        apiKey.setDescription(isDescriptionPresent() ? getDescription() : identity.getDescription());
        apiKey.setIssued(identity.getIssued());
        apiKey.setMaskedId(identity.getMaskedId());
        return apiKey;
    }
}
