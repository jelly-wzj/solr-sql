package com.jenkin.exceptions;

import java.sql.SQLException;

public class SolrSqlException extends SQLException {
	private static final long serialVersionUID = 1L;
	private String msg;
	private int code;

	public SolrSqlException() {
		super();
	}

	public SolrSqlException(String msg) {
		super(msg);
		this.msg = msg;
	}

	public SolrSqlException(int code, String msg) {
		super();
		this.msg = msg;
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

}
