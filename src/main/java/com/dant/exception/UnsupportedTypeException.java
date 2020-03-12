package com.dant.exception;

public class UnsupportedTypeException extends Exception {

    public UnsupportedTypeException(String msg){
        super("[ERROR] - Unsupported type exception, details : \n"+msg);
    }
}
