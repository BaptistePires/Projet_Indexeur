package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;

    public HashMap<Object, ArrayList<Integer>> data;

    @Expose
    private String name;
    @Expose
    private int columnNo;
    @Expose
    private String type;

    public Column(String name, String type) throws UnsupportedTypeException {
        this.name = name;
        columnNo = UNDEFINED_NO;
        this.type = type;
    }

    public void setColumnNo(int columnNo) {
        this.columnNo = columnNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getColumnNo() {
        return columnNo;
    }

    public String getType(){
        return type;
    }

    public void setType(String s){
        type = s;
    }

    public abstract Object insert(String s, int index);

    public abstract Object get(int i);


    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
