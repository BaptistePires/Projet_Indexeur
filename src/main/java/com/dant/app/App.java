package com.dant.app;

import com.dant.exception.*;
import com.dant.filter.GsonProvider;
import io.swagger.v3.jaxrs2.integration.resources.AcceptHeaderOpenApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pitton on 2017-02-20.
 */
@ApplicationPath("/api")
public class App extends Application {

    @Override
    public Set<Object> getSingletons() {
        Set<Object> sets = new HashSet<>(1);
        sets.add(new TestEndpoint());
        sets.add(new IndexerEndpoint());
        return sets;
    }

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> sets = new HashSet<>(1);
        sets.add(GsonProvider.class);
        sets.add(RuntimeExceptionMapper.class);
        sets.add(InvalidIndexException.class);
        sets.add(InvalidIndexExceptionMapper.class);
        sets.add(UnsupportedTypeExceptionMapper.class);
        sets.add(InvalidFileException.class);
        sets.add(InvalidFileExceptionMapper.class);
        sets.add(NoDataException.class);
        sets.add(NoDataExceptionMapper.class);
        sets.add(OpenApiResource.class);
        sets.add(AcceptHeaderOpenApiResource.class);
        return sets;
    }
}
