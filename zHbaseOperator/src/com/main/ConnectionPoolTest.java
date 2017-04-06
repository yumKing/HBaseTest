package com.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ConnectionPoolTest {
    private static final String QUORUM = "jinyang";
    private static final String CLIENTPORT = "2181";
    private static final String TABLENAME = "jin";
    private static Configuration conf = null;
    private static HConnection conn = null;
    
    static{
        try {
            conf =  HBaseConfiguration.create();  
            conf.set("hbase.zookeeper.quorum", QUORUM);   
            conf.set("hbase.zookeeper.property.clientPort", CLIENTPORT);  
            conn = HConnectionManager.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    

    public static void main(String[] args) throws Exception {
        HTableInterface htable = ConnectionPoolTest.conn.getTable(TABLENAME);
        try {
            Scan scan = new Scan();
            ResultScanner rs = htable.getScanner(scan);
            for (Result r : rs.next(5)) {
                for (Cell cell : r.rawCells()) {
                    System.out.println("Rowkey : " + Bytes.toString(r.getRow())
                            + "   Familiy:Quilifier : "
                            + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "   Value : "
                            + Bytes.toString(CellUtil.cloneValue(cell))
                            + "   Time : " + cell.getTimestamp());
                }
            }
        } finally {
            htable.close();
        }
        
    }
}