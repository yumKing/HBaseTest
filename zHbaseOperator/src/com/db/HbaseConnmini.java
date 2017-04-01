package com.db;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;

public class HbaseConnmini {

	//定义hbase配置管理器
	private static Configuration cfg = HBaseConfiguration.create();
	//定义配置文件的读取对象
	private static LoadConf lconf = null;
	//定义连接池集合
	private static ArrayList<HConnection> connLst = new ArrayList<>(); 
	
	//hbase表名的字符串化
	public static String basetest = null;
	
	//一系列初始化
	static{
		try {
			//一旦获取了该对象，配置文件就读取完毕
			lconf = new LoadConf();
			
			//获取hbase zookeeper地址和端口
			String addr = lconf.getProp("zookeeperaddr");
			String port = lconf.getProp("zookeeperport");
			
			//将其设置到hbase配置管理器中
			cfg.set("hbase.zookeeper.quorum", addr);
			cfg.set("zookeeper.properties.clientPort", port);
			
			//现在才开始获取配置文件中的表名
			basetest = lconf.getProp("htb_users");
			//创建hbase连接池
			createHconnection(Integer.valueOf(lconf.getProp("connect")));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**创建连接池
	 * @param connectNum
	 * @throws IOException
	 */
	public static void createHconnection(int connectNum) throws IOException{
		for(int i = 0 ; i < connectNum;i++){
			//设置hbase配置管理器的实例id
			cfg.setInt("hbase.client.instance.id",i);
			//创建hbase连接，并且将连接存入集合中
			connLst.add(HConnectionManager.createConnection(cfg));
		}
	}
	
	/**
	 * 获取连接池中的一个hbase连接
	 * @return
	 */
	public static HConnection getHConn(){
		int id = (int) (System.currentTimeMillis()%connLst.size());
		return connLst.get(id);
	}
	
	/**
	 *通过表名 获取hbase库中的表信息
	 * @param _tblName
	 * @return
	 * @throws IOException
	 */
	public static HTable getHBaseTable(String _tblName) throws IOException{
			return (HTable)getHConn().getTable(_tblName);
	}
	
	/**
	 * 通过配置文件获取需要的hbase的表名
	 * @param _key
	 * @return
	 */
	public static String getTblName(String _key){
		if(lconf.checkProp(_key)){
			return lconf.getProp(_key);
		}
		else{
			return "";
		}
	}
}
