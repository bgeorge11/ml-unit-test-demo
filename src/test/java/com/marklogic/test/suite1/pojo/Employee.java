package com.marklogic.test.suite1.pojo;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "employee")
@XmlType(propOrder = { "employeeId", "fName", "lName", "addresses" })
public class Employee {

	private int employeeId;
	private String fName;
	private String lName;
	private List<Address> addresses = null;

	public int getEmployeeId() {
		return employeeId;
	}

	@XmlElement(name = "employeeId")
	public void setEmployeeId(int employeeId) {
		this.employeeId = employeeId;
	}

	public String getfName() {
		return fName;
	}

	@XmlElement(name = "fName")
	public void setfName(String firstName) {
		this.fName = firstName;
	}

	public String getlName() {
		return lName;
	}

	@XmlElement(name = "lName")
	public void setlName(String lastName) {
		this.lName = lastName;
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	@XmlElementWrapper(name = "Addresses")
	@XmlElement(name = "Address")
	public void setAddresses(List<Address> addresses) {
		this.addresses = addresses;
	}

}
