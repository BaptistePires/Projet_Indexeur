package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public abstract class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;

    @Expose
    private String name;
    @Expose
    private int columnNo;
    @Expose
    private boolean isIndexed;

    private HashMap<Object, ArrayList<Integer>> index;

    public Column(String name) throws UnsupportedTypeException {
        this.name = name;
        columnNo = UNDEFINED_NO;
        isIndexed = false;
        index = null;
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

    public final void setIndexed(boolean status) {
        isIndexed = status;
        if(!isIndexed) return;
        index = new HashMap<>();
    }


    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
