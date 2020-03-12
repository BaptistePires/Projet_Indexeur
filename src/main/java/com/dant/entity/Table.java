package com.dant.entity;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {

    /**
     * columns : Each column of the table is represented by a Column object, Set is used because
     * we don't want any duplicated columns.
     */
    private Set<Column> columns;

    /**
     * columnMappedByName : Map indexing columns by their names, can be useful when we load a .csv
     * file and need to retrieve header's columns types.
     */
    private Map<String, Column> columnMappedByName;

    /**
     * indexes : Can be interpreted as a sub-set of columns, it contains references to columns
     * that are used to index data.
     */
    private Set<Column> indexes;

    public Table() {
        columns = new HashSet<>();
        columnMappedByName = new HashMap<>();
        indexes = new HashSet<>();
    }

    public void addColumn(Column c) {
        columns.add(c);
        columnMappedByName.put(c.getName(), c);
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
        columnMappedByName.remove(col.getName());
        indexes.remove(col);
    }

    public void removeIndex(String colName) {
        indexes.remove(getColumnByName(colName));
    }

    public void removeIndexedColByReference(Column col) {
        removeIndex(col.getName());
    }

    // Getters & Setters
    public Map<String, Column> getColumnMappedByName() {
        return columnMappedByName;
    }

    public Column getColumnByName(String name) {
        return columnMappedByName.get(name);
    }

    public Set<Column> getColumns() {
        return columns;
    }

    public List<String> getColumnsName() {
        return new ArrayList<>(columnMappedByName.keySet());
    }

    public Set<Column> getIndexedColumns() {
        return indexes;
    }
}
