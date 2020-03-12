package com.dant.entity;

import java.util.List;

public class Table {
    private List<String> cols;
    private List<String> rows;
    private List<String> indexes;


    public Table(List<String> cols) {
        this.cols = cols;
    }

    public Table(List<String> cols, List<String> indexes) {
        this.cols = cols;
        this.indexes = indexes;
    }


    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    public List<String> getRows() {
        return rows;
    }

    public void setRows(List<String> rows) {
        this.rows = rows;
    }

    public List<String> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<String> indexes) {
        this.indexes = indexes;
    }

    @Override
    public String toString() {
        return "Table{" +
                "cols=" + cols +
                ", rows=" + rows +
                ", indexes=" + indexes +
                '}';
    }
}
