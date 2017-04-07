package com;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {

	public static void main(String[] args) throws Exception {
		//创建默认的配置对象
		Configuration conf = HBaseConfiguration.create();
		//获取admin对象
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		//创建表描述
		//1、添加表名
		//2、天剑列簇名
		HTableDescriptor desc = new HTableDescriptor("twits");
		HColumnDescriptor cold = new HColumnDescriptor("twits");
		cold.setMaxVersions(1);//限定时间戳版本上限
		desc.addFamily(cold);
		
		//创建表
		admin.createTable(desc);
		
	}
	
}
