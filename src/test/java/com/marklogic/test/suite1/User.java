package com.marklogic.test.suite1;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

public class User  {

	private int employeeId;
	private String fName;
	private String lName;
	

	public int getEmployeeId() {
		return employeeId;
	}

	public void setEmployeeId(int employeeId) {
		this.employeeId = employeeId;
	}

	public String getfName() {
		return fName;
	}

	public void setfName(String firstName) {
		this.fName = firstName;
	}

	public String getlName() {
		return lName;
	}

	public void setlName(String lastName) {
		this.lName = lastName;
	}

}
