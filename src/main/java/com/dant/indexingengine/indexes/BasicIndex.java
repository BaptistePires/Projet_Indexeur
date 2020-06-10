package com.dant.indexingengine.indexes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public abstract class BasicIndex {



    public abstract void indexObject(Object o, int noLine) throws IOException;

    public abstract List<Integer> findLinesForObject(Object o, int limit) throws IOException;


}
