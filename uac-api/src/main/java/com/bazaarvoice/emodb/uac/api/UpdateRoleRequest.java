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

@JsonSerialize(using = UpdateRoleRequest.UpdateRoleRequestSerializer.class)
public class UpdateRoleRequest {
    private String _name;
    private boolean _namePresent;
    private String _description;
    private boolean _descriptionPresent;
    private Set<String> _grantedPermissions = Sets.newHashSet();
    private Set<String> _revokedPermissions = Sets.newHashSet();
    private boolean _revokeOtherPermissions;

    public String getName() {
        return _name;
    }

    public UpdateRoleRequest setName(String name) {
        _name = name;
        _namePresent = true;
        return this;
    }

    public boolean isNamePresent() {
        return _namePresent;
    }

    public String getDescription() {
        return _description;
    }

    public UpdateRoleRequest setDescription(String description) {
        _description = description;
        _descriptionPresent = true;
        return this;
    }

    public boolean isDescriptionPresent() {
        return _descriptionPresent;
    }

    public Set<String> getGrantedPermissions() {
        return ImmutableSet.copyOf(_grantedPermissions);
    }

    public UpdateRoleRequest setGrantedPermissions(Set<String> grantedPermissions) {
        _grantedPermissions.clear();
        _grantedPermissions.addAll(grantedPermissions);
        return this;
    }

    public UpdateRoleRequest grantPermissions(Set<String> grantedPermissions) {
        checkArgument(Sets.intersection(grantedPermissions, _revokedPermissions).isEmpty(),
                "Cannot both grant and revoke the same permission");
        _grantedPermissions.addAll(grantedPermissions);
        return this;
    }

    public Set<String> getRevokedPermissions() {
        return ImmutableSet.copyOf(_revokedPermissions);
    }

    public UpdateRoleRequest setRevokedPermissions(Set<String> revokedPermissions) {
        _revokedPermissions.clear();
        _revokedPermissions.addAll(revokedPermissions);
        return this;
    }

    public UpdateRoleRequest revokePermissions(Set<String> revokedPermissions) {
        checkArgument(Sets.intersection(revokedPermissions, _grantedPermissions).isEmpty(),
                "Cannot both grant and revoke the same permission");
        _revokedPermissions.addAll(revokedPermissions);
        return this;
    }

    public boolean isRevokeOtherPermissions() {
        return _revokeOtherPermissions;
    }

    public UpdateRoleRequest setRevokeOtherPermissions(boolean revokeOtherPermissions) {
        _revokeOtherPermissions = revokeOtherPermissions;
        return this;
    }

    /**
     * Custom serializer to omit values which have not been explicitly set.
     */
    static class UpdateRoleRequestSerializer extends JsonSerializer<UpdateRoleRequest> {
        @Override
        public void serialize(UpdateRoleRequest request, JsonGenerator gen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            gen.writeStartObject();
            if (request.isNamePresent()) {
                gen.writeStringField("name", request.getName());
            }
            if (request.isDescriptionPresent()) {
                gen.writeStringField("description", request.getDescription());
            }
            if (!request.getGrantedPermissions().isEmpty()) {
                gen.writeObjectField("grantedPermissions", request.getGrantedPermissions());
            }
            if (!request.getRevokedPermissions().isEmpty()) {
                gen.writeObjectField("revokedPermissions", request.getRevokedPermissions());
            }
            gen.writeBooleanField("revokeOtherPermissions", request.isRevokeOtherPermissions());
            gen.writeEndObject();
        }
    }
}
