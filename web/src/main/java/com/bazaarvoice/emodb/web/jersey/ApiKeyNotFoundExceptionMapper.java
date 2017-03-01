package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.ApiKeyNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class ApiKeyNotFoundExceptionMapper implements ExceptionMapper<ApiKeyNotFoundException> {
    @Override
    public Response toResponse(ApiKeyNotFoundException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", ApiKeyNotFoundException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
