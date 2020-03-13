package com.dant.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class InvalidIndexExceptionMapper implements ExceptionMapper<InvalidIndexException> {

    @Override
    public Response toResponse(InvalidIndexException e) {
        return Response.status(400).entity(e.getMessage()).type("plain/text").build();
    }
}
