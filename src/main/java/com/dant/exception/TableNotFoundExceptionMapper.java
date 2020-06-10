package com.dant.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class TableNotFoundExceptionMapper implements ExceptionMapper<TableNotFoundException> {

    @Override
    public Response toResponse(TableNotFoundException e) {
        return Response.status(400).entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    }
}
