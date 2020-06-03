package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;

    @Expose
    private String name;
    @Expose
    private int columnNo;


    public Column(String name) throws UnsupportedTypeException {
        this.name = name;
        columnNo = UNDEFINED_NO;
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


    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
