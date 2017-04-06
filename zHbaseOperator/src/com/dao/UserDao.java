package com.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.db.HbaseConnmini;

public class UserDao {

//	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	
	public static final byte[] FAM_INFO = Bytes.toBytes("info");
	
	public static final byte[] COL_USER = Bytes.toBytes("user");
	
	public static final byte[] COL_NAME = Bytes.toBytes("name");
	
	public static final byte[] COL_PASS = Bytes.toBytes("password");
	
	public static final byte[] COL_EMAIL = Bytes.toBytes("email");
	
	private static final Logger log = Logger.getLogger(UserDao.class);
	
	//增or更新
	public void addUser(String user,String name,String password,String email) throws Exception{
		
		log.debug(String.format("add user : %s", new User(user,name,password,email)));
		
		HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.users);
		
		
		Put put = new Put(user.getBytes());
		put.add(FAM_INFO, COL_USER, user.getBytes());
		put.add(FAM_INFO, COL_NAME, name.getBytes());
		put.add(FAM_INFO, COL_PASS, password.getBytes());
		put.add(FAM_INFO, COL_EMAIL, email.getBytes());
		
		users.put(put);
		
		users.close();
		
	}
	
	//删
	public void delUser(String user) throws Exception{
		log.debug(String.format("delete user : %s", user));
		HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.users);
		Delete del = new Delete(user.getBytes());
		users.delete(del);
		users.close();
	}
	
	//查
	public com.model.User getUser(String user) throws Exception{
		log.debug(String.format("select user : %s", user));
		HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.users);
		Get get = new Get(user.getBytes());
		Result r = users.get(get);
		
		User u = new User(r);
		return u;
		
	}
	
	public List<com.model.User> getUsers() throws Exception{
		HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.users);
		
		ArrayList<com.model.User> ret = new ArrayList<com.model.User>();
		
		Scan scan = new Scan();
		scan.addFamily(FAM_INFO);
		ResultScanner scanner = users.getScanner(scan);
		for(Result r : scanner){
			ret.add(new User(r));
		}
		
		return ret;
	}
	
	
	private static class User extends com.model.User{
		
		private User(Result r){
			this(r.getValue(FAM_INFO, COL_USER),r.getValue(FAM_INFO, COL_NAME),r.getValue(FAM_INFO, COL_PASS),r.getValue(FAM_INFO, COL_EMAIL));
		}
		
		private User(byte[] user,byte[] name,byte[] password,byte[] email ){
			this(user.toString(),name.toString(),password.toString(),email.toString());
		}
		
		private User(String user,String name,String password,String email){
			this.setUser(user);
			this.setName(name);
			this.setPassword(password);
			this.setEmail(email);
		}
		
	}
	
}
