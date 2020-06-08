package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

import java.io.IOException;
import java.io.RandomAccessFile;

public class IntegerColumn extends Column {

    int max, min;
    double avg;

    public IntegerColumn(String name) throws UnsupportedTypeException {
        super(name);
        max = min = 0;
        avg = 0d;
    }

    @Override
    public Object castAndUpdateMetaData(String o) {
        int x = Integer.parseInt(o);
        if(x > max) max = x;
        else if(x < min) min = x;
        avg += x;
        avg /= 2.;

        return x;
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
