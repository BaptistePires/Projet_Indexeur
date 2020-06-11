package com.dant.exception;

public class NonIndexedColumn extends Exception {
    public NonIndexedColumn(String colName) {
        super(colName + " is not indexed");
    }
}
