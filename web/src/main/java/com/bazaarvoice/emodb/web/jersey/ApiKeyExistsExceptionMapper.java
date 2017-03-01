package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.ApiKeyExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class ApiKeyExistsExceptionMapper implements ExceptionMapper<ApiKeyExistsException> {
    @Override
    public Response toResponse(ApiKeyExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", ApiKeyExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
