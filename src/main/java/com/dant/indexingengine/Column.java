package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public abstract class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;
    public final static int INT_BYTE_SIZE = 4;
    public final static int DOUBLE_BYTE_SIZE = 8;


    @Expose
    private String name;
    @Expose
    private int columnNo;
    @Expose
    private boolean isIndexed;

    private SimpleIndex index;

    public Column(String name) throws UnsupportedTypeException {
        this.name = name;
        columnNo = UNDEFINED_NO;
        isIndexed = false;
        index = null;
    }

    public void setColumnNo(int columnNo) {
        this.columnNo = columnNo;
    }

    public String getName() {
        return name;
    }

    public int getColumnNo() {
        return columnNo;
    }

    public abstract Object castAndUpdateMetaData(String o);

    public final void setIndexed() {
        isIndexed = true;
        index = new SimpleIndex();
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public void index(Object o, int noLine) throws IOException {
        if (isIndexed()) {
            index.index(o, noLine);
        }
    }

    public ArrayList<Integer> getLinesForIndex(Object o, int limit) throws IOException {
        if(isIndexed()) {
            return new ArrayList<>(index.get(o, limit));
        }
        // TODO : Handle non-indexed columns (linear search ?)
        return new ArrayList<>();
    }

    public abstract int writeToFile(RandomAccessFile file, Object o) throws IOException;

    public abstract Object readFromFile(RandomAccessFile file) throws IOException;

    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
