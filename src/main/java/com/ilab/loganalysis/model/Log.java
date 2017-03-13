package com.ilab.loganalysis.model;

import java.io.Serializable;

public class Log implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String timeMicros;
	private String cIp;
	private String cIpType;
	private String cIpRegion;
	private String csMethod;
	private String cs_uri;
	private String scStatus;
	private String scBytes;
	private String csBytes;
	private String timeTakenMicros;
	private String csHost;
	private String csReferer;
	private String csUserAgent;
	private String sRequestId;
	private String csOperation;
	private String csBucket;
	private String csObject;

	public String getTimeMicros() {
		return timeMicros;
	}

	public void setTimeMicros(String timeMicros) {
		this.timeMicros = timeMicros;
	}

	public String getcIp() {
		return cIp;
	}

	public void setcIp(String cIp) {
		this.cIp = cIp;
	}

	public String getcIpType() {
		return cIpType;
	}

	public void setcIpType(String cIpType) {
		this.cIpType = cIpType;
	}

	public String getcIpRegion() {
		return cIpRegion;
	}

	public void setcIpRegion(String cIpRegion) {
		this.cIpRegion = cIpRegion;
	}

	public String getCsMethod() {
		return csMethod;
	}

	public void setCsMethod(String csMethod) {
		this.csMethod = csMethod;
	}

	public String getCs_uri() {
		return cs_uri;
	}

	public void setCs_uri(String cs_uri) {
		this.cs_uri = cs_uri;
	}

	public String getScStatus() {
		return scStatus;
	}

	public void setScStatus(String scStatus) {
		this.scStatus = scStatus;
	}

	public String getCsBytes() {
		return csBytes;
	}

	public void setCsBytes(String csBytes) {
		this.csBytes = csBytes;
	}

	public String getScBytes() {
		return scBytes;
	}

	public void setScBytes(String scBytes) {
		this.scBytes = scBytes;
	}

	public String getTimeTakenMicros() {
		return timeTakenMicros;
	}

	public void setTimeTakenMicros(String timeTakenMicros) {
		this.timeTakenMicros = timeTakenMicros;
	}

	public String getCsHost() {
		return csHost;
	}

	public void setCsHost(String csHost) {
		this.csHost = csHost;
	}

	public String getCsReferer() {
		return csReferer;
	}

	public void setCsReferer(String csReferer) {
		this.csReferer = csReferer;
	}

	public String getCsUserAgent() {
		return csUserAgent;
	}

	public void setCsUserAgent(String csUserAgent) {
		this.csUserAgent = csUserAgent;
	}

	public String getsRequestId() {
		return sRequestId;
	}

	public void setsRequestId(String sRequestId) {
		this.sRequestId = sRequestId;
	}

	public String getCsOperation() {
		return csOperation;
	}

	public void setCsOperation(String csOperation) {
		this.csOperation = csOperation;
	}

	public String getCsBucket() {
		return csBucket;
	}

	public void setCsBucket(String csBucket) {
		this.csBucket = csBucket;
	}

	public String getCsObject() {
		return csObject;
	}

	public void setCsObject(String csObject) {
		this.csObject = csObject;
	}
}
