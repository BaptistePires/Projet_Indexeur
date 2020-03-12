package com.dant.entity;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Table implements Serializable {

    private Set<Column> columns;

    public Table(){
        this.columns = new HashSet<>();
    }

    public void addColumn(Column c){
        columns.add(c);
    }

    public Set<Column> getColumns() {
        return columns;
    }

    public void setColumns(Set<Column> columns) {
        this.columns = columns;
    }
}
