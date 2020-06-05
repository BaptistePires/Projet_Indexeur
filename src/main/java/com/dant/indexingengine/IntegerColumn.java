package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;

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




}
