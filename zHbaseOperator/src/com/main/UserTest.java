package com.main;

import com.dao.UserDao;

public class UserTest {

//	@SuppressWarnings("deprecation")
//	private static UserDao userDao = new UserDao(new HTablePool(HbaseConnmini.cfg, HbaseConnmini.maxSize));
	
	public static void main(String[] args) throws Exception {
		UserDao userDao = new UserDao();
		userDao.addUser("zhang","zhangsan","zhang123","zhang@163.com");
	}
}
