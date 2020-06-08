package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

import java.io.IOException;
import java.io.RandomAccessFile;

public class StringColumn extends Column {

    public StringColumn(String name) throws UnsupportedTypeException {
        super(name);
    }

    @Override
    public Object castAndUpdateMetaData(String o) {
        return o;
    }

    @Override
    public int writeToFile(RandomAccessFile file, Object o) throws IOException {
        long lengthBefore = file.length();
        file.writeUTF((String) o);
        return (int) (file.length() - lengthBefore);
    }

    @Override
    public Object readFromFile(RandomAccessFile file) throws IOException {
        return file.readUTF();
    }
}
