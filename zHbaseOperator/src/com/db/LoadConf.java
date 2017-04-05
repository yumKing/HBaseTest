package com.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class LoadConf {

	//定义属性对象
	private Properties prop = new Properties();
	
	//调用此类时直接将配置文件粗怒properties对象中
	public LoadConf() throws IOException{
		readConf();
	}
	
	private void  readConf() throws IOException{
		//获取要读取的属性文件的路径
		String pathSrc = LoadConf.class.getResource("").getPath();
		String path = pathSrc.substring(1, pathSrc.length())+"hbaseconfig.properties";
		
		//读取文件到文件对象中
		File file = new File(path);
		try {
			//将文件对象放入properties对象中
			prop.load(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			System.out.println("hbase配置文件文件没有发现");
		}
		
	}
	
	
	/**检查配置文件中是否存在该键值对
	 * @param _key
	 * @return
	 */
	public boolean checkProp(String _key){
		return prop.containsKey(_key);
	}
	
	/**
	 * @param _key
	 * @return
	 */
	public String getProp(String _key){
		return prop.getProperty(_key);
	}
}
