package com.marklogic.test.suite1.pojo;

import javax.xml.bind.annotation.XmlElement;

public class Address {

	private char addrType;
	private String addrLine1;
	private String addrLine2;
	private String city;
	private String state;
	private int postalCode;

	public char getAddrType() {
		return addrType;
	}

	@XmlElement(name = "addrType")
	public void setAddrType(char addrType) {
		this.addrType = addrType;
	}

	public String getAddrLine1() {
		return addrLine1;
	}

	@XmlElement(name = "addrLine1")
	public void setAddrLine1(String addrLine1) {
		this.addrLine1 = addrLine1;
	}

	public String getAddrLine2() {
		return addrLine2;
	}

	public void setAddrLine2(String addrLine2) {
		this.addrLine2 = addrLine2;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(int postalCode) {
		this.postalCode = postalCode;
	}

}
