package com.dant.exception;

public class InvalidIndexException extends Exception {

	public InvalidIndexException() {
		super("InvalidIndexException");
	}

	public InvalidIndexException(String msg){
		super("InvalidIndexException: The following columns don't belong to the table: " + msg);
	}
}
