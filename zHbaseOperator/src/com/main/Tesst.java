package com.main;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import com.db.HbaseConnmini;
import com.db.HbaseOperator;

public class Tesst {

	public static void puts(String[] args) throws IOException {
		ArrayList<HashMap<String,HashMap<String,String>>> data = new ArrayList<>();
		HashMap<String,String> map1 = new HashMap<>();
		HashMap<String,String> map2 = new HashMap<>();
		HashMap<String,HashMap<String,String>> listmap = new HashMap<>();
		
		map1.put("keytag", "rowkey3");//map1中存放该行的行健
		map2.put("test1", "this is a number");
		map2.put("test2", "100");//map2中可以添加多个名值对
		
		
		listmap.put("keytag", map1);//这个map存放行标识
		listmap.put("d",map2);//这个map存放列
		data.add(listmap);
		
		
		
		HbaseOperator.put(HbaseConnmini.getTblName("htb_jinyang"), data, "keytag");
	}
	
	public static void main(String[] args) throws IOException, NumberFormatException, ParseException {
//		HbaseOperator.get(HbaseConnmini.getTblName("htb_jinyang"), "rowkey1");
//		HbaseOperator.scan(HbaseConnmini.getTblName("htb_jinyang"), "rowkey2","rowkey3");
//		HbaseOperator.get(HbaseConnmini.getTblName("htb_base"), "9223370585895609503");
		String preTimeStamps = "20160501000000";
		String sufixTimeStamps = "20171212240000";
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
		 String startkey = String.valueOf(9223372036854775807L - Long.parseLong(format.parse(sufixTimeStamps).getTime() + "999999"));
	        String endkey = String.valueOf(9223372036854775807L - Long.parseLong(format.parse(preTimeStamps).getTime() + "000000"));
	        System.out.println(startkey);
	        System.out.println(endkey);
	        System.out.println(Long.MAX_VALUE);
	}
}
