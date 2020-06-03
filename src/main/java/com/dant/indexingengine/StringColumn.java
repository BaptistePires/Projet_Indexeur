package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

public class StringColumn extends Column {

    public StringColumn(String name) throws UnsupportedTypeException {
        super(name);
    }

    @Override
    public Object castAndUpdateMetaData(String o) {
        return o;
    }


}
