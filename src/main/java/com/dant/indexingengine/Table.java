package com.dant.indexingengine;

import com.dant.indexingengine.columns.Column;
import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Table implements Serializable {

    /**
     * columns : Each column of the table is represented by a Column object, Set is used because
     * we don't want any duplicated columns.
     */
    @Expose
    private final ArrayList<Column> columns;

    /**
     * columnMappedByName : Map indexing columns by their names, can be useful when we load a .csv
     * file and need to retrieve header's columns types.
     */
    private final Map<String, Column> columnsMappedByName;


    private final Map<Integer, Column> columnsMappedByNo;


    @Expose
    private String name;

    public Table() {
        this("");
    }

    public Table(String name) {
        columns = new ArrayList<>();
        columnsMappedByName = new HashMap<>();
        columnsMappedByNo = new HashMap<>();
        this.name = name;
    }

    public void addColumn(Column c) {
        columns.add(c);
        columnsMappedByName.put(c.getName(), c);
    }

    public void removeColumnByName(String name) {
        removeColumnByReference(getColumnByName(name));
    }

    public void removeColumnByReference(Column col) {
        // As there are no pointers, we need to remove manually references to object
        // in each list
        columns.remove(col);
        columnsMappedByName.remove(col.getName());
    }

    public Column getColumnByName(String name) {
        return columnsMappedByName.get(name);
    }

    public ArrayList<Column> getColumnsByNames(List<String> names) {
        return (ArrayList<Column>) names
                .stream()
                .map(this::getColumnByName)
                .collect(Collectors.toList());
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

    public List<String> getColumnsName() {
        return columns
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    public Column getColumnByNo(int no) {
        return columns.get(no);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void sortColumnsByNo() {
        columns.sort(Comparator.comparing(Column::getColumnNo));
    }

    public ArrayList<Column> getIndexedColumns() {
        ArrayList<Column> tmp = new ArrayList<>();
        for (Column c : columns) {
            if (c.isIndexed()) tmp.add(c);
        }
        return tmp;
    }
}
