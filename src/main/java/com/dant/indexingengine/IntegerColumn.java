package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

public class IntegerColumn extends Column {

    int max, min;
    double avg;

    public IntegerColumn(String name, String type) throws UnsupportedTypeException {
        super(name, type);
        max = min = 0;
        avg = 0d;
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
