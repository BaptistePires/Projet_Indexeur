package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

public class StringColumn extends Column {

    public StringColumn(String name, String type) throws UnsupportedTypeException {
        super(name, type);
    }

    @Override
    public Object insert(String s, int index) {
        return null;
    }

    @Override
    public Object get(int i) {
        return null;
    }
}
