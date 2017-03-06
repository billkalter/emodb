package com.bazaarvoice.emodb.auth.identity;

/**
 * Manager for identities.
 */
public interface AuthIdentityManager<T extends AuthIdentity> extends AuthIdentityReader<T> {

    /**
     * Creates an identity.
     * @throws IdentityExistsException if either the provided authentication ID or identity's internal ID are already in use.
     */
    void createIdentity(String authenticationId, T identity) throws IdentityExistsException;
    
    /**
     * Updates an identity.
     */
    void updateIdentity(T identity) throws IdentityNotFoundException;

    /**
     * Migrates an identity to a new authentication ID.
     * @throws IdentityNotFoundException if no identity matching the internal ID exists
     * @throws IdentityExistsException if another identity matching the authentication ID exists
     */
    void migrateIdentity(String internalId, String newAuthenticationId)throws IdentityNotFoundException, IdentityExistsException;

    /**
     * Deletes an identity.
     */
    void deleteIdentity(String internalId);
}
