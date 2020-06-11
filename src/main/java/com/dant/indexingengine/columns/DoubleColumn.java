package com.dant.indexingengine.columns;

import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.indexes.NumberIndex;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DoubleColumn extends Column {

    public DoubleColumn(String name) throws UnsupportedTypeException {
        super(name);
        index = new NumberIndex(this);
    }

    @Override
    public Object castAndUpdateMetaData(String o) {
        double x = Double.parseDouble(o);
        return x;
    }

    @Override
    public Object castString(String s) {
        return Double.parseDouble(s);
    }

    @Override
    public int writeToFile(RandomAccessFile file, Object o) throws IOException {
        file.writeDouble((double) o);
        return DOUBLE_BYTE_SIZE;
    }

    @Override
    public Object readFromFile(RandomAccessFile file) throws IOException {
        return file.readDouble();
    }
}
