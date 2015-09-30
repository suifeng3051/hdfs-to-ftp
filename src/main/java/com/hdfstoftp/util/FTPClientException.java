package com.hdfstoftp.util;

import java.io.IOException;

/**
 * 自定义异常类，FTPClient异常
 * @author heaven
 */
public class FTPClientException extends Exception {

	private static final long serialVersionUID = 1L;

	public FTPClientException(String string, IOException e) {
		super(string,e);
	}
	public FTPClientException(String string) {
		super(string);
	}
	
}
