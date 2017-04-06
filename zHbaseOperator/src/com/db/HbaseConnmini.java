package com.db;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;

public class HbaseConnmini {

	//定义hbase配置管理器
	public static Configuration cfg = HBaseConfiguration.create();
	//定义配置文件的读取对象
	private static LoadConf lconf = null;
	
	static ArrayList<HConnection> listHconn = new ArrayList<HConnection>();
	
	//hbase表名的字符串化
	public static String users = null;
	
	//一系列初始化
	static{
		try {
			//一旦获取了该对象，配置文件就读取完毕
			lconf = new LoadConf();
			
			//获取hbase zookeeper地址和端口
			String addr = lconf.getProp("serverip");
			String port = lconf.getProp("serverport");
			
			//将其设置到hbase配置管理器中
			cfg.set("hbase.zookeeper.quorum", addr);
			cfg.set("hbase.zookeeper.properties.clientPort", port);
			
			//现在才开始获取配置文件中的表名
			users = lconf.getProp("htb_users");
			//创建hbase连接池
			createHconnection(Integer.valueOf(lconf.getProp("connect")));
			System.out.println("初始化成功");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static HTable getHBaseTable(String _tblname) throws Exception {
		// System.out.println("_tblname:"+_tblname);
		return (HTable) getConn().getTable(_tblname);
	}

	public static String getHtblName(String _key) {
		if (lconf.checkProp(_key))
			return lconf.getProp(_key);
		else
			return "";
	}

	public static void createHconnection(int _num) throws Exception {
		for (int i = 0; i < _num; i++) {
			cfg.setInt("hbase.client.instance.id", i);
			listHconn.add(HConnectionManager.createConnection(cfg));
		}
	}

	public static HConnection getConn() {
		int id = (int) (System.currentTimeMillis() % listHconn.size());
		return listHconn.get(id);
	}
	
}
