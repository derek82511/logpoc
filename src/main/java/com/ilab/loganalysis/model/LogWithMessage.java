package com.ilab.loganalysis.model;

import java.io.Serializable;

public class LogWithMessage implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Log data;
	private Message message;

	public LogWithMessage(Log data, Message message) {
		this.data = data;
		this.message = message;
	}

	public Log getData() {
		return data;
	}

	public Message getMessage() {
		return message;
	}
}
