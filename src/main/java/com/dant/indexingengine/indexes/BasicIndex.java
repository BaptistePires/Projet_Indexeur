package com.dant.indexingengine.indexes;

import java.io.IOException;
import java.util.List;

public abstract class BasicIndex {


    public abstract void indexObject(Object o, int noLine) throws IOException;

    public abstract List<Integer> findLinesForObjectEquals(Object o, int limit) throws IOException;

    public abstract List<Integer> findLinesForObjectSuperior(Object o, int limit) throws IOException;
}
