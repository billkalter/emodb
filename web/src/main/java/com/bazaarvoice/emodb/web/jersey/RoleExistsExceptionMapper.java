package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.RoleExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class RoleExistsExceptionMapper implements ExceptionMapper<RoleExistsException> {
    @Override
    public Response toResponse(RoleExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", RoleExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}