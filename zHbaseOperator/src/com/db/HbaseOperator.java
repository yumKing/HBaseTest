package com.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class HbaseOperator {

	/**
	 * 通用hbase存放数据方法
	 * @param _htlName
	 * @param dataList
	 * @param keytag
	 * @throws IOException
	 */
	public static void put(String _htlName,ArrayList<HashMap<String,HashMap<String,String>>> dataList,String keytag) throws IOException{
		HTable hTable = HbaseConnmini.getHBaseTable(_htlName);
		ArrayList<Put> putLst = new ArrayList<>();
		
		for(HashMap<String,HashMap<String,String>> listmap : dataList){
			
			//一行的数据
			//行标识
			String hkey = listmap.get(keytag).get(keytag);
			Put put = new Put(hkey.getBytes());
			
			for(Entry<String,HashMap<String,String>> listentry:listmap.entrySet()){
				//列簇名
				String cfname = listentry.getKey();
				if(cfname.equals(keytag)){
					continue;
				}
				
				for(Entry<String,String> entry : listentry.getValue().entrySet()){
					put.add(cfname.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
				}
			}
			
			//将每行数据存入集合
			putLst.add(put);
		}
		
		hTable.put(putLst);
		hTable.flushCommits();
		hTable.close();
		System.out.println("存放成功或修改成功");
	}
	
	/**
	 * 根据指定的rowkey获取指定表的一行数据
	 * @throws IOException 
	 * 
	 */
	public static void get(String _htlName,String _seqrkey) throws IOException{
		if(checkTableinHbase(_htlName, _seqrkey)){
			HTable hTable = HbaseConnmini.getHBaseTable(_htlName);
			Get get = new Get(_seqrkey.getBytes());
			Result result = hTable.get(get);
			
			String rowkey = new String(result.getRow());
			System.out.println("行键:"+rowkey);
			for(Cell cl : result.listCells()){
				String qualifier = new String(CellUtil.cloneQualifier(cl));
				String family = new String(CellUtil.cloneFamily(cl));
				String value = new String(CellUtil.cloneValue(cl));
				
				System.out.println("列簇名:"+family+",字段名:"+qualifier+",字段值:"+value);
			}
			System.out.println("=====================");
			hTable.close();
			System.out.println("查询成功");
		}
	}
	
	/**
	 * 添加多个参数来限定扫描表的信息
	 * @param agrs
	 */
	public static void scan(HashMap<String,String> agrs){
		//TODO 解析map，将参数取出
		
		//TODO 操作hbase
	}
	
	/**
	 * 根据限定行键范围来扫描表的信息，即获取几行数据
	 * @param _htlName
	 * @param _startRow
	 * @param _stopRow
	 * @throws IOException
	 */
	public static void scan(String _htlName,String _startRow,String _stopRow) throws IOException{
		
		
		HTable hTable = HbaseConnmini.getHBaseTable(_htlName);
		
		Scan scan = new Scan();
		//设置扫描的起始行和终止行
		scan.setStartRow(_startRow.getBytes());
		scan.setStopRow(_stopRow.getBytes());
		
		//设置缓存大小
		scan.setCaching(100);
		
		//设置过滤器，这里暂时不考虑
		scan.setFilter(null);
		
		ResultScanner scanner = hTable.getScanner(scan);
		for(Result result : scanner){
			String rowkey = new String( result.getRow());
			System.out.println("行键:"+rowkey);
			for(Cell cl : result.listCells()){
				String family = new String(CellUtil.cloneFamily(cl));
				String qualifier = new String(CellUtil.cloneQualifier(cl));
				String value = new String(CellUtil.cloneValue(cl));
				
				System.out.println("列簇名:"+family+",字段名:"+qualifier+",字段值:"+value);
				
			}
			System.out.println("===================");
		}
		
		hTable.close();
		System.out.println("扫描完毕");
	}
	
	/**
	 * hbase删除通用方法
	 * @param _htlName
	 * @param _seqrkey
	 * @throws IOException
	 */
	public static void delete(String _htlName,String _seqrkey) throws IOException{
		
		if(checkTableinHbase(_htlName, _seqrkey)){
			HTable hBaseTable = HbaseConnmini.getHBaseTable(_htlName);
			Delete del = new Delete(_seqrkey.getBytes());
			hBaseTable.delete(del);
			hBaseTable.flushCommits();
			hBaseTable.close();
			System.out.println("删除成功");
		}
	}
	
	/**
	 * 检查hbase中是否存在这个表
	 * @param _htlName
	 * @param _seqrkey
	 * @return
	 * @throws IOException
	 */
	public static boolean checkTableinHbase(String _htlName,String _seqrkey) throws IOException{
		HTable hTable = HbaseConnmini.getHBaseTable(_htlName);
		Get get = new Get(_seqrkey.getBytes());
		Result result = hTable.get(get);
		
		boolean flag = false;
		if(!result.isEmpty()){
			flag =  true;
		}
		hTable.close();
		return flag;
	}
}
