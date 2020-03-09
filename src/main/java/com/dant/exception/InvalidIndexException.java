package com.dant.exception;

public class InvalidIndexException extends Exception {
	public InvalidIndexException(String msg){
		super("Invalid Index : " + msg);
	}
	public InvalidIndexException(){
		super("Invalid Index.");
	}
}
