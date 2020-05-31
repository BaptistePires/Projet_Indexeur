package com.dant.entity;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {

    /**
     * columns : Each column of the table is represented by a Column object, Set is used because
     * we don't want any duplicated columns.
     */
    @Expose
    private Set<Column> columns;

    /**
     * columnMappedByName : Map indexing columns by their names, can be useful when we load a .csv
     * file and need to retrieve header's columns types.
     */
    private Map<String, Column> columnsMappedByName;


    private Map<Integer, Column> columnsMappedByNo;

    /**
     * indexes : Can be interpreted as a sub-set of columns, it contains references to columns
     * that are used to index data.
     */
    @Expose
    private Set<Column> indexes;

    @Expose
    private String name;

    public Table() {
        columns = new HashSet<>();
        columnsMappedByName = new HashMap<>();
        columnsMappedByNo = new HashMap<>();
        indexes = new HashSet<>();
        name = "";
    }

    public void addColumn(Column c) {
        columns.add(c);
        columnsMappedByName.put(c.getName(), c);
    }

    public void addIndexByName(String id) {
        indexes.add(getColumnByName(id));
    }

    public void removeColumnByName(String name) {
        removeColumnByReference(getColumnByName(name));
    }

    public void removeColumnByReference(Column col) {
        // As there are no pointers, we need to remove manually references to object
        // in each list
        columns.remove(col);
        columnsMappedByName.remove(col.getName());
        indexes.remove(col);
    }

    public void removeIndex(String colName) {
        indexes.remove(getColumnByName(colName));
    }

    public void removeIndexedColByReference(Column col) {
        removeIndex(col.getName());
    }

    public Column getColumnByName(String name) {
        return columnsMappedByName.get(name);
    }

    public Set<Column> getColumns() {
        return columns;
    }

    public List<String> getColumnsName() {
        return new ArrayList<>(columnsMappedByName.keySet());
    }

    public Set<Column> getIndexedColumns() {
        return indexes;
    }

    public Column getColumnByNo(int no) {
        return columnsMappedByNo.get(no);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * This method must be called once you set up ALL of the columns numbers.
     */
    public void mapColumnsByNo() throws Exception {
        int number;
        for (Column c : columns) {
            number = c.getColumnNo();
            if (number == Column.UNDEFINED_NO) {
                String s = "Column : " + c.getName() + " has no column number";
                throw new Exception(s);
            }
            columnsMappedByNo.put(c.getColumnNo(), c);
        }
    }
}
