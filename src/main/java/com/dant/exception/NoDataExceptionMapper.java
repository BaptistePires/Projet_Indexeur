package com.dant.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class NoDataExceptionMapper implements ExceptionMapper<NoDataException> {

    @Override
    public Response toResponse(NoDataException e) {
        return Response.status(404).entity(e.getMessage()).type("plain/text").build();
    }

}
