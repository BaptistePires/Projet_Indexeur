package com.dant.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class InvalidFileExceptionMapper implements ExceptionMapper<InvalidFileException> {

    @Override
    public Response toResponse(InvalidFileException e) {
        return Response.status(400).entity(e.getMessage()).type("plain/text").build();
    }
}
