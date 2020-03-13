package com.dant.exception;

public class InvalidFileException extends Exception {

    public InvalidFileException() {
        super("InvalidIndexException");
    }

    public InvalidFileException(String msg) {
        super("InvalidFileException: File extension does not match expected (.csv) : " + msg);
    }

}
