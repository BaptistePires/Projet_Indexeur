package com.dant.entity;

import com.dant.exception.UnsupportedTypeException;

import java.io.Serializable;
import java.util.function.Function;

public class Column implements Serializable {

    private String name;
    private Function<String, ?> typeCaster;
    private String strType;
    private int columnNo;

    public Column(String name, String type) throws UnsupportedTypeException {
        this.name = name;
        columnNo = -1;
        strType = type;
        setUpTypeCaster();
    }

    // Currently supporting only Integers and Strings
    public void setUpTypeCaster() throws UnsupportedTypeException {
        switch (strType) {
            case "Integer":
                typeCaster = Integer::parseInt;
                break;

            case "String":
                typeCaster = String::new;
                break;

            default:
                throw new UnsupportedTypeException("Type not supported : " + strType);
        }
    }

    public Object getTypeCaster(String s) {
        return typeCaster.apply(s);
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


    @Override
    public int hashCode() {
        int hash = 11;
        hash += 139 * (name == null ? 0 : name.length());
        hash += 139 * (typeCaster == null ? 0 : typeCaster.hashCode());
        return hash;
    }

    @Override
    public String toString() {
        return "{name:" + getName() + ", typeCaster";
    }
}
