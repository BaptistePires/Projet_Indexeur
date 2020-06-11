package com.dant.exception;

public class TableNotFoundException extends Exception {

    public TableNotFoundException() {
        super("TableNotFoundException : There is no table matching given name.");
    }

    public TableNotFoundException(String msg) {
        super("TableNotFoundException : There is no table matching given name, " + msg);
    }

}
