package com.dant.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class UnsupportedTypeExceptionMapper implements ExceptionMapper<UnsupportedTypeException> {

    @Override
    public Response toResponse(UnsupportedTypeException e) {
        return Response.status(400).entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    }
}
