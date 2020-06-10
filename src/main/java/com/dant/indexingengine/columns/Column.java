package com.dant.indexingengine.columns;

import com.dant.exception.NonIndexedColumn;
import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.indexes.BasicIndex;
import com.dant.indexingengine.indexes.HashIndex;
import com.google.gson.annotations.Expose;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;

public abstract class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;
    public final static int INT_BYTE_SIZE = 4;
    public final static int DOUBLE_BYTE_SIZE = 8;


    @Expose
    private final String name;
    @Expose
    private int columnNo;
    @Expose
    private boolean isIndexed;

    protected BasicIndex index;

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

    public abstract Object castString(String s);

    public final void setIndexed() {
        isIndexed = true;
        index = new HashIndex();
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public void index(Object o, int noLine) throws IOException {
        if (isIndexed()) {
            index.indexObject(o, noLine);
        }
    }

    /**
     * @param o     Value the row should have
     * @param limit Maximum number of lines returned
     * @return List of lines
     * @throws IOException
     */
    public ArrayList<Integer> getLinesForIndex(Object o, int limit) throws IOException, NonIndexedColumn {

        if (isIndexed()) {
            if (this instanceof IntegerColumn) {
                if (o instanceof Integer) return new ArrayList<>(index.findLinesForObject(o, limit));
                // Else if Double cast to Integer
                return new ArrayList<>(index.findLinesForObject(((Double) o).intValue(), limit));
            } else {
                return new ArrayList<>(index.findLinesForObject(o, limit));
            }

        }

        throw new NonIndexedColumn(name);
    }

    public abstract int writeToFile(RandomAccessFile file, Object o) throws IOException;

    public abstract Object readFromFile(RandomAccessFile file) throws IOException;

    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
