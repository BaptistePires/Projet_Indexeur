package com.dant.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class WrongFileFormatExceptionMapper implements ExceptionMapper<WrongFileFormatException> {

    @Override
    public Response toResponse(WrongFileFormatException exception) {
        return  Response.status(400).entity(exception.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    }
}
