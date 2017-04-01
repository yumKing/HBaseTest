package com.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.db.HbaseConnmini;
import com.model.Split;
import com.model.User;

public class UserOperator {

	// public static final byte[] TABLE_NAME = Bytes.toBytes("users");

	public static final byte[] FAM_INFO = Bytes.toBytes("info");

	public static final byte[] USER_COL = Bytes.toBytes("user");

	public static final byte[] EMAIL_COL = Bytes.toBytes("email");

	public static final byte[] NAME_COL = Bytes.toBytes("name");

	public static final byte[] PASS_COL = Bytes.toBytes("password");

	public static final byte[] TWITS_COL = Bytes.toBytes("twits");

	/**
	 * 并行计算扫描hbase中的数据，基本测试的线程数是8个，当然仍然是在一台电脑上执行
	 * 由于扫描需要时间，如果数据容量很大，初期的并行还是可以，但后期数据越来越大就不行了
	 * 这是后就需要把人物分配到多台电脑上去分开执行，这样将数据细分下来就将时间减少了
	 * 
	 * 问题：
	 * 1、线程卡死
	 * 2、硬盘损坏，电脑故障
	 * 3、数据切片出现问题，其他电脑是否会出现问题
	 * 4、聚合工作如何进行，哪些切片完成任务，哪些又没有完成
	 * 5、计算工作的记录该如何存放
	 * 
	 * 解决：hadoop分布式
	 * 除了计算工作和聚合工作，其他工作hadoop统统都自动解决
	 */
	public static void scanIO() {

		int splitNum = 8;// 切片数量
		String startrow = "";
		String endrow = "";
		// 拆分工作
		Split[] splits = Split.split(startrow, endrow, splitNum);
		// 分发工作
		List<Future<?>> works = new ArrayList<>(splitNum);
		ExecutorService es = Executors.newFixedThreadPool(splitNum);
		for (final Split split : splits) {
			works.add(es.submit(new Runnable() {

				@Override
				public void run() {
					try {
						HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);
						Scan scan = new Scan(split.getStart().getBytes(), split.getEnd().getBytes());
						
						ResultScanner results = users.getScanner(scan);
						for(Result r :results){
							//计算工作
							//TODO
						}
						
						
						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
					
					

				}
			}));
		}
		
		
		for(Future f :works ){
			try {
				f.get();
				//聚合工作
				//TODO
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void scan() {
		try {
			HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);

			Scan scan = new Scan();
			// scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("password"));

			// 过滤，是吧要过滤的内容取出来(可能吧不同列页过滤出来)
			// 1、正则过滤
			// Filter filter = new ValueFilter(CompareOp.EQUAL, new
			// RegexStringComparator(".*ya.*"));
			// 2、编译过滤
			// String exp = "ValueFilter(=,'regexstring:.*1234.*')";
			// ParseFilter p = new ParseFilter();
			// Filter filter =
			// p.parseSimpleFilterExpression(Bytes.toBytes(exp));
			// 3、值过滤
			// FilterList filterList = new
			// FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter filter = new SingleColumnValueFilter("info".getBytes(), "password".getBytes(),
					CompareOp.EQUAL, "1234".getBytes());
			// SingleColumnValueFilter filter1 = new
			// SingleColumnValueFilter("info".getBytes(), "password".getBytes(),
			// CompareOp.EQUAL,new RegexStringComparator(".*ya.*"));
			filter.setFilterIfMissing(true);

			// 4、行键过滤(前缀过滤，起始终止行设置过滤)
			// byte[] prefix = "jin".getBytes();
			// Filter filter = new PrefixFilter(prefix);

			// filterList.addFilter(filter);

			scan.setFilter(filter);

			ResultScanner scanner = users.getScanner(scan);
			for (Result rt : scanner) {
				System.out.println("行键：" + Bytes.toString(rt.getRow()));
				System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
				List<Cell> listCells = rt.listCells();

				for (Cell cl : listCells) {
					System.out.println("列簇名：" + new String(CellUtil.cloneFamily(cl)));
					System.out.println("列名：" + new String(CellUtil.cloneQualifier(cl)));
					System.out.println("值：" + new String(CellUtil.cloneValue(cl)));
					System.out.println("================================");
				}

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void insert(User user) {
		try {
			HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);

			Put put = new Put(Bytes.toBytes(user.getUser()));// 添加行键
			if (user.getName() != null) {
				put.add(FAM_INFO, NAME_COL, Bytes.toBytes(user.getName()));// 添加列簇，列名，值
			}
			if (user.getEmail() != null) {
				put.add(FAM_INFO, EMAIL_COL, Bytes.toBytes(user.getEmail()));// 添加列簇，列名，值
			}
			if (user.getPassword() != null) {
				put.add(FAM_INFO, PASS_COL, Bytes.toBytes(user.getPassword()));// 添加列簇，列名，值
			}
			if (user.getTwits() != null) {
				put.add(FAM_INFO, TWITS_COL, Bytes.toBytes(user.getTwits()));// 添加列簇，列名，值
			}

			if (put.size() > 0) {
				users.put(put);
				users.close();
				System.out.println("存hbase成功");
			} else {
				users.close();
				System.out.println("没有添加任何数据");
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void update() {
		try {
			HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);
			Put put = new Put(Bytes.toBytes("TheRealMT"));// 添加行键
			put.add(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));// 添加列簇，列名，值
			users.put(put);
			users.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void get() {
		try {
			HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);

			Get get = new Get(Bytes.toBytes("TheRealMT"));
			get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
			get.setMaxVersions();
			Result rt = users.get(get);

			// byte[] value =
			// rt.getValue(Bytes.toBytes("info"),Bytes.toBytes("password"));
			// System.out.println(Bytes.toString(value));

			System.out.println("行键：" + Bytes.toString(rt.getRow()));
			System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
			List<Cell> cells = rt.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("password"));
			// System.out.println(cells.size());
			for (Cell cl : cells) {
				System.out.println("时间戳：" + cl.getTimestamp() + ",转换后："
						+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cl.getTimestamp()));
				System.out.println("列簇名：" + new String(CellUtil.cloneFamily(cl)));
				System.out.println("列名：" + new String(CellUtil.cloneQualifier(cl)));
				System.out.println("值：" + new String(CellUtil.cloneValue(cl)));
				System.out.println("================================");
			}
			users.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void delete() {
		try {
			HTableInterface users = HbaseConnmini.getHBaseTable(HbaseConnmini.basetest);
			Delete del = new Delete(Bytes.toBytes("TheRealMT"));
			// 可以指定删除指定数据或者行
			del.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("email"));
			users.delete(del);
			users.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// User user = new User();
		// user.setUser("yang");
		// user.setEmail("123@qq.com");
		// user.setName("yang");
		// user.setPassword("yang");
		// insert(user);
		scan();
	}
}
