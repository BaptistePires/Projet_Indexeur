package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

public class DoubleColumn extends Column {

    public DoubleColumn(String name) throws UnsupportedTypeException {
        super(name);
    }

    @Override
    public Object castAndUpdateMetaData(String o) {
        double x = Double.parseDouble(o);
        return x;
    }
}
