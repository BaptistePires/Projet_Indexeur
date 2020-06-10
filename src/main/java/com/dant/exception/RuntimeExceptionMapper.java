package com.dant.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Created by pitton on 2017-02-21.
 */
@Provider
public class RuntimeExceptionMapper implements ExceptionMapper<RuntimeException> {

    @Override
    public Response toResponse(RuntimeException e) {
        e.printStackTrace();
        return Response.status(400).entity(e.getLocalizedMessage()).type("plain/text").build();
    }
}
