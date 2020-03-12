package com.dant.entity;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;

import java.io.Serializable;

public class Column implements Serializable {

    private String name;
    private Class<?> typeCaster;
    private int columnNo;

    public Column(String name, String type) throws UnsupportedTypeException {
        this.name = name;
        columnNo = -1;
        setUpTypeCaster(type);
    }

    // Currently supporting only Integers and Strings
    public void setUpTypeCaster(String type) throws UnsupportedTypeException {
        switch (type){
            case "Integer":
                typeCaster = Integer.class;
                break;

            case "String":
                typeCaster = String.class;
                break;

            default:
                throw new UnsupportedTypeException("Type not supported : " + type);
        }
    }

    public Class<?> getTypeCaster(){
        return typeCaster;
    }

    public void setColumnNo(int columnNo){
        this.columnNo = columnNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTypeCaster(Class<?> typeCaster) {
        this.typeCaster = typeCaster;
    }

    public int getColumnNo() {
        return columnNo;
    }



    @Override
    public int hashCode() {
        int hash = 11;
        hash += 139 * (name == null ? 0 : name.length());
        hash += 139 * (typeCaster == null ? 0 : typeCaster.hashCode());
        return hash;
    }

    @Override
    public String toString() {
        return "{name:"+getName()+", typeCaster";
    }
}
