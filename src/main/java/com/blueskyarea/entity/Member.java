package com.blueskyarea.entity;

import java.io.Serializable;

public class Member implements Serializable {
	private static final long serialVersionUID = -4510179567125782194L;
	private String name;
	private String address;
	private String phoneNumber;
	private String mailAddress;
	
	public Member(String name, String address, String phoneNumber, String mailAddress) {
		this.name = name;
		this.address = address;
		this.phoneNumber = phoneNumber;
		this.mailAddress = mailAddress;
	}
	
	// should be specified for default constructor
	public Member() {
		this("test", "test", "test", "test");
	}
	
	public String getName() {
		return name;
	}
	
	public String getAddress() {
		return address;
	}
	
	public String getPhoneNumber() {
		return phoneNumber;
	}
	
	public String getMailAddress() {
		return mailAddress;
	}
	
	// setter are mandatory for encoder
	public void setName(String name) {
		this.name = name;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}
	
	public void setMailAddress(String mailAddress) {
		this.mailAddress = mailAddress;
	}
}
