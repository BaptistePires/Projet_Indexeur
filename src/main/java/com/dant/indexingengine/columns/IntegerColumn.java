package com.dant.indexingengine.columns;

import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.indexes.NumberIndex;

import java.io.IOException;
import java.io.RandomAccessFile;

public class IntegerColumn extends Column {

    int max, min;
    double avg;

    public IntegerColumn(String name) throws UnsupportedTypeException {
        super(name);
        max = min = 0;
        avg = 0d;
        index = new NumberIndex(this);
    }

    @Override
    public Object castAndUpdateMetaData(String s) {
        int x = (int) castString(s);
        if (x > max) max = x;
        else if (x < min) min = x;
        avg += x;
        avg /= 2.;
        return x;
    }

    @Override
    public Object castString(String s) {
        return s.equalsIgnoreCase("") ? 0 : (int) Double.parseDouble(s);

    }

    @Override
    public int writeToFile(RandomAccessFile file, Object o) throws IOException {
        file.writeInt((int) o);
        return INT_BYTE_SIZE;
    }

    @Override
    public Object readFromFile(RandomAccessFile file) throws IOException {
        return file.readInt();
    }
}
