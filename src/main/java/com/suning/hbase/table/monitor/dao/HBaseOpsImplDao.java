package com.suning.hbase.table.monitor.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.model.ClusterStruct;
import com.suning.hbase.table.monitor.model.ClusterTotalRequest;
import com.suning.hbase.table.monitor.model.ClusterfamilyStruct;
import com.suning.hbase.table.monitor.model.TableParam;
import com.suning.hbase.table.monitor.model.TableStruct;
import com.suning.hbase.table.monitor.model.columnfamilyStruct;
import com.suning.hbase.util.HBaseUtil;

public class HBaseOpsImplDao {
  private static Logger LOG = LoggerFactory.getLogger(HBaseOpsImplDao.class);
  private static ThreadLocal<HConnection> tlconnection = new ThreadLocal<HConnection>();
  
  
  
  public  HConnection getConnection() throws IOException {
    HConnection conn = tlconnection.get();
    if (conn == null) { 
        conn = HBaseUtil.getConnection(); 
        tlconnection.set(conn);    
    }
    return conn;
  }
  
  public void setConnection(HConnection conn ){
    tlconnection.set(conn); 
  }
  
  public  void close() {
    try {
      HConnection conn = getConnection();
      if (conn != null) {
        try {
          conn.close();
        } finally {
          tlconnection.remove();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
  
  public  void doInsert(ClusterTotalRequest clusterparam,String nowtime,HConnection connection){
    String tablename= HBaseUtil.getClusterTable();
    HTableInterface hbasttable = null;
    try {
      hbasttable =connection.getTable(tablename);    
     Put put = new Put(Bytes.toBytes(nowtime)); 
     LOG.info("-------------------------------------------------");
     LOG.info(hbasttable.getName().getNameAsString()+" doInsert begion");
    
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.starttime), Bytes.toBytes(clusterparam.getStartTime()));
     // requestcount > readrequestcount+writerequestcount �������ݲ�ͬ����read and write ���ݸ��²�û��tatol write����ͨ
     //put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.requestcount), Bytes.toBytes(Long.toString(clusterparam.getClusterRequestCount())));
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.requestcount), Bytes.toBytes(Long.toString(clusterparam.getClusterReadRequestCount()+clusterparam.getClusterWriteRequestCount())));
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.readrequestcount), Bytes.toBytes(Long.toString(clusterparam.getClusterReadRequestCount())));
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.writerequestcount), Bytes.toBytes(Long.toString(clusterparam.getClusterWriteRequestCount())));
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.regionnum), Bytes.toBytes(Long.toString(clusterparam.getClusterregionnum())));
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.useheapMb), Bytes.toBytes(Long.toString(clusterparam.getClusteruseheapMb())));
   //  System.out.println(clusterparam.getClustermaxheapM());
     put.add(Bytes.toBytes(ClusterStruct.clusterfamily), Bytes.toBytes(ClusterfamilyStruct.maxheapMb), Bytes.toBytes(Long.toString(clusterparam.getClustermaxheapM())));
     hbasttable.put(put);
     LOG.info(hbasttable.getName().getNameAsString()+" doInsert end");
     LOG.info("-------------------------------------------------");
   } catch (IOException e) {
     e.printStackTrace();
   }
    
  }
  @SuppressWarnings({ "static-access", "static-access" })
  public  void doInsert(HashMap<String, HashMap<String, String>> param,String nowtime,HConnection connection){
      List<Put> plist = new ArrayList<Put>();
    String tablename= HBaseUtil.getHBaseTable();
    HTableInterface hbasttable = null;
    try {
       hbasttable =connection.getTable(tablename);
      hbasttable.setAutoFlush(true, true);
      int writeBuffer = 1024 * 1024;
      if (writeBuffer != 0) {
        hbasttable.setWriteBufferSize(writeBuffer);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
        
    Iterator<String> it = param.keySet().iterator();
    HashMap<String, String> tableParam= new HashMap<String, String>();
    while(it.hasNext()){
      String key = it.next();
      System.out.println(key +":");
      tableParam  = param.get(key);
      if(!key.equals("hbase:acl" )&& !key.equals("hbase:meta") && !key.equals("hbase:namespace") && tableParam.containsKey(TableParam.regionNum)){    
      System.out.print(key +":");
      tableParam  = param.get(key); 
      Put put = new Put(Bytes.toBytes(key+nowtime));    
      LOG.info("totalRequestCount:"+tableParam.get(TableParam.totalRequestCount));
      LOG.info("TableParam.regionNum:"+tableParam.get(TableParam.regionNum));
      LOG.info("TableParam.storefileSizeMB:"+tableParam.get(TableParam.storefileSizeMB));
      LOG.info("TableParam.MemStoreSizeMB:"+tableParam.get(TableParam.MemStoreSizeMB));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(columnfamilyStruct.requestcount), Bytes.toBytes(tableParam.get(TableParam.totalRequestCount)));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(columnfamilyStruct.writerequestcount), Bytes.toBytes(tableParam.get(TableParam.writeRequestCount)));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(TableStruct.cfs.readrequestcount), Bytes.toBytes(tableParam.get(TableParam.readRequestCount)));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(TableStruct.cfs.filesize), Bytes.toBytes(tableParam.get(TableParam.storefileSizeMB)));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(TableStruct.cfs.memsize), Bytes.toBytes(tableParam.get(TableParam.MemStoreSizeMB)));
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(TableStruct.cfs.regionnum), Bytes.toBytes(tableParam.get(TableParam.regionNum)));
      LOG.info("coprocessor:"+tableParam.get(TableParam.coprocessor));
      if(!tableParam.containsKey(TableParam.coprocessor)){
        LOG.info("table:"+key + "  no  coprocessor");
       
      }
      put.add(Bytes.toBytes(TableStruct.columnfamilyname), Bytes.toBytes(TableStruct.cfs.coprocessor), Bytes.toBytes(tableParam.get(TableParam.coprocessor)));  
      plist.add(put);
    }  
    }
    try {
      hbasttable.put(plist);
      hbasttable.flushCommits();
      LOG.info("insert put list to hbase");
    } catch (IOException e) {
      LOG.error("doInsert  put  error");
      e.printStackTrace();
    }
    
  }
  public  static  HashMap<String, String> doSelectTest(String tablename,String near5mtime,String newtime,HConnection connection) throws IOException
  {
    String dbname= HBaseUtil.getHBaseTable();
    HashMap<String, String> near5mBytable = new HashMap<String, String>();
    HTableInterface hbasttable = connection.getTable(dbname);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(tablename+near5mtime));
    scan.setStopRow(Bytes.toBytes(tablename+newtime));
    ResultScanner results = hbasttable.getScanner(scan);
    for (Result result : results) {        
      System.out.println("test"+Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.requestcount.getBytes())));
      near5mBytable.put(TableParam.totalRequestCount, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.requestcount.getBytes())));
      near5mBytable.put(TableParam.writeRequestCount, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.writerequestcount.getBytes())));
      near5mBytable.put(TableParam.readRequestCount, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.readrequestcount.getBytes())));
      near5mBytable.put(TableParam.regionNum, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.regionnum.getBytes())));
      near5mBytable.put(TableParam.coprocessor, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.coprocessor.getBytes())));
      near5mBytable.put(TableParam.storefileSizeMB, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.filesize.getBytes())));
      near5mBytable.put(TableParam.MemStoreSizeMB, Bytes.toString(result.getValue(TableStruct.columnfamilyname.getBytes(), columnfamilyStruct.memsize.getBytes())));
      // ����һ�Σ�������ݷ�������ѡ�����µ�����
     // break;         
   }
    return near5mBytable;
}
  public ResultScanner doSelectByTimeRange(String tablename,String starttime,String endtime,HConnection connection){
    String dbname= HBaseUtil.getHBaseTable();
    columnfamilyStruct  cs = new columnfamilyStruct();
    HTableInterface hbasttable;
    ResultScanner results = null;
    Scan scan;
    try {
       hbasttable = connection.getTable(dbname);
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes(tablename+starttime));
        scan.setStopRow(Bytes.toBytes(tablename+endtime));
        results = hbasttable.getScanner(scan);
    } catch (IOException e) {
      e.printStackTrace();
    }   
    return results;
  }
  public ResultScanner doSelectByTimeRange(String starttime,String endtime,HConnection connection){
    String dbname= HBaseUtil.getClusterTable();
    HTableInterface hbasttable;
    ResultScanner results = null;
    Scan scan;
    try {
       hbasttable = connection.getTable(dbname);
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes(starttime));
        scan.setStopRow(Bytes.toBytes(endtime));
        results = hbasttable.getScanner(scan);
    } catch (IOException e) {
      e.printStackTrace();
    }   
    return results;
  }
  public ResultScanner doSelect(String tablename,String near5mtime,String nowtime,HConnection connection) throws IOException
  {
    String dbname= HBaseUtil.getHBaseTable();
    HTableInterface hbasttable = connection.getTable(dbname);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(tablename+near5mtime));
    scan.setStopRow(Bytes.toBytes(tablename+nowtime));
    ResultScanner results = hbasttable.getScanner(scan);
   /* for (Result result : results) {
      
      for(Cell cell:result.rawCells()){ 
      
        String rowname= new String(CellUtil.cloneRow(cell));
        String[] rownamearrys = rowname.split("2015");   //��������Ż�   
        near5mtime= rownamearrys[1];
        switch(new String(CellUtil.cloneQualifier(cell))){    
        case "rqc":
              near5mBytable.put(TableParam.totalRequestCount,new String(CellUtil.cloneValue(cell)));
              break;
        case  "wrqc":
               near5mBytable.put(TableParam.writeRequestCount,new String(CellUtil.cloneValue(cell)));
               break;
        case  "rrqc":
              near5mBytable.put(TableParam.readRequestCount,new String(CellUtil.cloneValue(cell)));
              break;
        case  "rnum": 
               near5mBytable.put(TableParam.regionNum,new String(CellUtil.cloneValue(cell)));
               break;
        case  "fsize":   
               near5mBytable.put(TableParam.storefileSizeMB,new String(CellUtil.cloneValue(cell)));
               break;
        case  "msize":   
               near5mBytable.put(TableParam.MemStoreSizeMB,new String(CellUtil.cloneValue(cell)));
               break;
        case  "cp":
               near5mBytable.put(TableParam.coprocessor,new String(CellUtil.cloneValue(cell)));
               break;
        default:
            LOG.error("ERROR match "+new String(CellUtil.cloneQualifier(cell)));     
        }
        System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
        System.out.println("Timetamp:"+cell.getTimestamp()+" ");
        System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
        System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
        System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");

      }
      // ����һ�Σ�������ݷ�������ѡ�����µ�����
     break;         
   }*/   
    return results;
}
  public static void main(String[] args) throws IOException {
    HBaseOpsImplDao  opsdao = new HBaseOpsImplDao();
     HConnection connect = opsdao.getConnection();
     HashMap<String, String> near5mBytable = doSelectTest("master1114", "2015-03-26 18:04:36", "2015-03-26 18:29:36", connect);
    connect.close();    
  }
}
  