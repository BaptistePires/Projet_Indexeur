package com.dant.entity;

import com.dant.exception.UnsupportedTypeException;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.function.Function;

public class Column implements Serializable {

    public final static int UNDEFINED_NO = -1;

    @Expose
    private String name;
    private Function<String, ?> typeCaster;
    private Type type;
    @Expose
    private String strType;
    @Expose
    private int columnNo;

    public Column(String name, String type) throws UnsupportedTypeException {
        this.name = name;
        columnNo = UNDEFINED_NO;
        strType = type;
        setUpTypeCaster();
    }

    // Currently supporting only Integers and Strings
    public void setUpTypeCaster() throws UnsupportedTypeException {
        switch (strType) {
            case "Integer":
                // Need to cast to double to parse int because of "1.0"......................
                // Quick & dirty tmp
                typeCaster = (s) -> s.contains(".") ? Integer.parseInt(s.split("\\.")[0]) : s.length() > 0 ? Integer.parseInt(s) : 0;
                type = new TypeToken<Integer>() {
                }.getType();
                break;

            case "String":
                typeCaster = String::new;
                type = new TypeToken<String>() {
                }.getType();

                break;

            default:
                throw new UnsupportedTypeException("Type not supported : " + strType);
        }
    }

    public Object castStringToType(String s) {
        return typeCaster.apply(s);
    }

    public Type getType() {
        return this.type;
    }

    public Function<String, ?> getCastingFunction() {
        return typeCaster;
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
