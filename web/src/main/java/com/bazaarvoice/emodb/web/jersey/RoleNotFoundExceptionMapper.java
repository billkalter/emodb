package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.RoleNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class RoleNotFoundExceptionMapper implements ExceptionMapper<RoleNotFoundException> {
    @Override
    public Response toResponse(RoleNotFoundException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", RoleNotFoundException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
