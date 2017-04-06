package com.model;

public abstract class User {

	public String user;
	public String email;
	public String name;
	public String password;
	public String getTwits() {
		return twits;
	}

	public void setTwits(String twits) {
		this.twits = twits;
	}

	public String twits;
	
	@Override
	public String toString() {
		return String.format("<User:%s,%s,%s,%s,%s>", user,email,name,password,twits);
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
}
