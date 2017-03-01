package com.bazaarvoice.emodb.uac.api;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@JsonSerialize(using = UpdateApiKeyRequest.UpdateApiKeyRequestSerializer.class)
public class UpdateApiKeyRequest {

    private String _owner;
    private boolean _ownerPresent;
    private String _description;
    private boolean _descriptionPresent;
    private Set<RoleIdentifier> _assignedRoles = Sets.newHashSet();
    private Set<RoleIdentifier> _unassignedRoles = Sets.newHashSet();
    private boolean _unassignRemainingRoles;
    
    public String getOwner() {
        return _owner;
    }

    public UpdateApiKeyRequest setOwner(String owner) {
        _owner = owner;
        _ownerPresent = true;
        return this;
    }

    public boolean isOwnerPresent() {
        return _ownerPresent;
    }

    public String getDescription() {
        return _description;
    }

    public UpdateApiKeyRequest setDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public Set<RoleIdentifier> getAssignedRoles() {
        return ImmutableSet.copyOf(_assignedRoles);
    }

    public UpdateApiKeyRequest setAssignedRoles(Set<RoleIdentifier> assignedRoles) {
        _assignedRoles.clear();
        _assignedRoles.addAll(assignedRoles);
        return this;
    }

    public UpdateApiKeyRequest assignRoles(Set<RoleIdentifier> addedRoles) {
        checkNotNull(addedRoles, "roles");
        checkArgument(Sets.intersection(addedRoles, _unassignedRoles).isEmpty(),
                "Cannot both assign and unassign the same role");
        _assignedRoles.addAll(addedRoles);
        return this;
    }

    public Set<RoleIdentifier> getUnassignedRoles() {
        return ImmutableSet.copyOf(_unassignedRoles);
    }

    public UpdateApiKeyRequest setUnassignedRoles(Set<RoleIdentifier> unassignedRoles) {
        _unassignedRoles.clear();
        _unassignedRoles.addAll(unassignedRoles);
        return this;
    }

    public UpdateApiKeyRequest unassignRoles(Set<RoleIdentifier> removedRoles) {
        checkNotNull(removedRoles, "roles");
        checkArgument(Sets.intersection(removedRoles, _assignedRoles).isEmpty(),
                "Cannot both assign and unassign the same role");
        _unassignedRoles.addAll(removedRoles);
        return this;
    }

    public boolean isUnassignRemainingRoles() {
        return _unassignRemainingRoles;
    }

    public UpdateApiKeyRequest setUnassignRemainingRoles(boolean unassignRemainingRoles) {
        _unassignRemainingRoles = unassignRemainingRoles;
        return this;
    }

    /**
     * Custom serializer to omit nullable values which have not been explicitly set.
     */
    static class UpdateApiKeyRequestSerializer extends JsonSerializer<UpdateApiKeyRequest> {
        @Override
        public void serialize(UpdateApiKeyRequest request, JsonGenerator gen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            gen.writeStartObject();
            if (request.isOwnerPresent()) {
                gen.writeStringField("owner", request.getOwner());
            }
            if (request.isDescriptionPresent()) {
                gen.writeStringField("description", request.getDescription());
            }
            if (!request.getAssignedRoles().isEmpty()) {
                gen.writeObjectField("assignedRoles", request.getAssignedRoles());
            }
            if (!request.getUnassignedRoles().isEmpty()) {
                gen.writeObjectField("unassignedRoles", request.getUnassignedRoles());
            }
            gen.writeBooleanField("unassignRemainingRoles", request.isUnassignRemainingRoles());
            gen.writeEndObject();
        }
    }
}
