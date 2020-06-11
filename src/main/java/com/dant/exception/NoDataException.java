package com.dant.exception;

public class NoDataException extends Exception {

    public NoDataException() {
        super("NoDataException : There are no results for your query.");
    }

    public NoDataException(String msg) {
        super("NoDataException : There are no results for your query, " + msg);
    }

}
