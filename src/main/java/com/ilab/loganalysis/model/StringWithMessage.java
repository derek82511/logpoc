package com.ilab.loganalysis.model;

import java.io.Serializable;

public class StringWithMessage implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String data;
	private Message message;

	public StringWithMessage(String data, Message message) {
		this.data = data;
		this.message = message;
	}

	public String getData() {
		return data;
	}

	public Message getMessage() {
		return message;
	}
}
