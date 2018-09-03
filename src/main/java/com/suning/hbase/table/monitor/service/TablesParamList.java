package com.suning.hbase.table.monitor.service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.table.monitor.model.ErrorFlag;
import com.suning.hbase.table.monitor.model.ResultTablesParamStruct;
import com.suning.hbase.table.monitor.model.TableParam;
import com.suning.hbase.util.ArithmeticUtil;
import com.suning.hbase.util.HBaseTimeUtil;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.HBaseUtilWithNoInit;

public class TablesParamList {
  
  public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  //private static HashMap<String, HashMap<String, String>> hbaseTableParam =new HashMap<String, HashMap<String, String>>();
  //private static HashMap<String, HashMap<String, String>> resultTableParam =new HashMap<String, HashMap<String, String>>();
  private static HashMap<String, HashMap<String, String>> hbaseRealTimeTableParam =new HashMap<String, HashMap<String, String>>();
  private static String near5mtime ;
  private static String clusterstarttime;
  private static Logger LOG = LoggerFactory.getLogger(TablesParamList.class);
  
  
  
  public HashMap<String, String>  getBefore5mParamByTable(String tablename,String nowtime,HConnection connect, HBaseOpsImplDao  opsdao){
    HashMap<String, String> near5mBytable = new HashMap<String, String>();
     near5mtime = HBaseTimeUtil.subDate(nowtime, -5); 
     clusterstarttime =HBaseUtil.getClusterStartTime();
      //select hbase table para
     
      try {
        ResultScanner results = opsdao.doSelect(tablename, near5mtime,nowtime,connect);
        for (Result result : results) {
          
          for(Cell cell:result.rawCells()){ 
          
            String rowname= new String(CellUtil.cloneRow(cell));
            // 
             near5mtime= rowname.substring(tablename.length()) ;  //后面继续优化   
            near5mBytable.put(TableParam.tableName,new String(CellUtil.cloneRow(cell)));
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
          }
          // 遍历一次，别的数据放弃掉，选择最新的数据
         break;         
       }  
        //   5 minutes before no collect data,
  /*     if(near5mBytable.isEmpty()){
     
        near5mBytable.put(TableParam.totalRequestCount,new String("0"));
        near5mBytable.put(TableParam.writeRequestCount,new String("0"));
        near5mBytable.put(TableParam.readRequestCount,new String("0"));
        near5mBytable.put(TableParam.MemStoreSizeMB,new String("0"));
        near5mBytable.put(TableParam.storefileSizeMB,new String("0"));
        near5mBytable.put(TableParam.readRequestCount,new String("0"));
        near5mBytable.put(TableParam.coprocessor,new String("0"));
      }*/
      } catch (IOException e) {    
        e.printStackTrace();
      }
     
    return near5mBytable;
  }
  
  public  void CaptureTablesRealTimeParam(ArrayList<HTableDescriptor> tables,HashMap<String, HashMap<String, String>> hbaseRealTimeTableParam) {
    HBaseUtil.getTablesCoprocessor(tables, hbaseRealTimeTableParam);
    HBaseUtil.getTablesParam(hbaseRealTimeTableParam); 
   
  }
  public HashMap<String,HashMap<String,String>> CaptureTablesRealTimeParam(ArrayList<HTableDescriptor> tables,
			HashMap<String, HashMap<String, String>> hbaseRealTimeTableParam2,
			Configuration conf) {
	     HBaseUtilWithNoInit.getTablesCoprocessor(tables, hbaseRealTimeTableParam);
	     System.out.println(hbaseRealTimeTableParam.size());
	     HBaseUtilWithNoInit.getTablesParam(hbaseRealTimeTableParam,conf); 
	     System.out.println(hbaseRealTimeTableParam.size());
	     return hbaseRealTimeTableParam;
	}
  public   HashMap<String,HashMap<String,String>>  getRegionServersParam(){  
    return HBaseUtil.getRegionServerParam();   
  }
  
  public  HashMap<String,HashMap<String,String>> getTablesParamList(HConnection connect) throws IOException{
    HashMap<String, HashMap<String, String>> resultTableParam =new HashMap<String, HashMap<String, String>>();
    String nowtime=  df.format(new Date());
    ArrayList<HTableDescriptor> tables = HBaseUtil.getAllTable();      
    CaptureTablesRealTimeParam(tables,hbaseRealTimeTableParam);
    HashMap<String, String> tableRealTime;
    HashMap<String, String> tableNearBy5mParam;
    int  flag  =0 ;
    long totalrequestcount = 0;
    long writerequestcount = 0;
    long readrequestcount = 0;
    if(connect.isClosed()){
      connect =HBaseUtil.getConnection();
    }
    HBaseOpsImplDao  opsdao = new HBaseOpsImplDao();   
    for(HTableDescriptor table:tables){ 
      String tablename=table.getNameAsString();
       HashMap<String, String> resulttableParam = new HashMap<String, String>() ;
       tableNearBy5mParam=getBefore5mParamByTable(tablename,nowtime,connect,opsdao);
       
       tableRealTime  = hbaseRealTimeTableParam.get(tablename);     
       
      if(tableNearBy5mParam.size() !=(tableRealTime.size()+1)){
        // no data  before 5 min
    	  
        if(tableNearBy5mParam.size()==0 ){
         LOG.warn(tablename + " near 5 min has no data in hbase");
          flag = 1;
          if(null ==tableRealTime.get(TableParam.regionNum)){
            flag = -1; 
            
            totalrequestcount= -1;
            writerequestcount= -1;    
            readrequestcount=  -1; 
            
          } else{
            totalrequestcount= Long.parseLong(tableRealTime.get(TableParam.totalRequestCount));
            writerequestcount= Long.parseLong(tableRealTime.get(TableParam.writeRequestCount));    
            readrequestcount=  Long.parseLong(tableRealTime.get(TableParam.readRequestCount));
            near5mtime= clusterstarttime;
           }
        }
        else{//add  disable table and bad tables
          LOG.error(table.getNameAsString()+":tableNearBy5mParam  vaule no equal tableRealTime"); 
          flag = -1; 
        }
                      
      }
      
      if(flag ==0){
      totalrequestcount= -Long.parseLong(tableNearBy5mParam.get(TableParam.totalRequestCount))+Long.parseLong(tableRealTime.get(TableParam.totalRequestCount));
      writerequestcount= -Long.parseLong(tableNearBy5mParam.get(TableParam.writeRequestCount))+Long.parseLong(tableRealTime.get(TableParam.writeRequestCount));    
      readrequestcount=  -Long.parseLong(tableNearBy5mParam.get(TableParam.readRequestCount))+Long.parseLong(tableRealTime.get(TableParam.readRequestCount));
      long intervaltime = HBaseTimeUtil.subDate(nowtime, near5mtime)/1000;    
      resulttableParam.put(ResultTablesParamStruct.qps,Double.toString( ArithmeticUtil.div(totalrequestcount, intervaltime)));
      resulttableParam.put(ResultTablesParamStruct.wqps,Double.toString( ArithmeticUtil.div(writerequestcount, intervaltime)));
      resulttableParam.put(ResultTablesParamStruct.rqps,Double.toString( ArithmeticUtil.div(readrequestcount, intervaltime)));
     
      resulttableParam.put(ResultTablesParamStruct.regionNum,tableRealTime.get(TableParam.regionNum));
      resulttableParam.put(ResultTablesParamStruct.memsize,tableRealTime.get(TableParam.MemStoreSizeMB));
      resulttableParam.put(ResultTablesParamStruct.filesize,tableRealTime.get(TableParam.storefileSizeMB));
      resulttableParam.put(ResultTablesParamStruct.coprocessor,tableRealTime.get(TableParam.coprocessor));
      LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
      resultTableParam.put(tablename, resulttableParam); 
      flag  =0 ;
      }else if(flag ==-1){
        resulttableParam.put(ResultTablesParamStruct.qps,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.wqps,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.rqps,ErrorFlag.disableTable);      
        resulttableParam.put(ResultTablesParamStruct.regionNum,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.memsize,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.filesize,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.coprocessor,ErrorFlag.disableTable);
        LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
        resultTableParam.put(tablename, resulttableParam);  
        flag  =0 ;
      }else{
        long intervaltime = HBaseTimeUtil.subDate(nowtime, near5mtime)/1000;    
        resulttableParam.put(ResultTablesParamStruct.qps,Double.toString( ArithmeticUtil.div(totalrequestcount, intervaltime)));
        resulttableParam.put(ResultTablesParamStruct.wqps,Double.toString( ArithmeticUtil.div(writerequestcount, intervaltime)));
        resulttableParam.put(ResultTablesParamStruct.rqps,Double.toString( ArithmeticUtil.div(readrequestcount, intervaltime)));
       
        resulttableParam.put(ResultTablesParamStruct.regionNum,tableRealTime.get(TableParam.regionNum));
        resulttableParam.put(ResultTablesParamStruct.memsize,tableRealTime.get(TableParam.MemStoreSizeMB));
        resulttableParam.put(ResultTablesParamStruct.filesize,tableRealTime.get(TableParam.storefileSizeMB));
        resulttableParam.put(ResultTablesParamStruct.coprocessor,tableRealTime.get(TableParam.coprocessor));
        LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
        resultTableParam.put(tablename, resulttableParam); 
        
      }
      
    }
    return resultTableParam;
   
    
  }
  public  HashMap<String,HashMap<String,String>> getTablesParamList() throws IOException{
    HashMap<String, HashMap<String, String>> resultTableParam =new HashMap<String, HashMap<String, String>>();
    String nowtime=  df.format(new Date());
    ArrayList<HTableDescriptor> tables = HBaseUtil.getAllTable();      
    CaptureTablesRealTimeParam(tables,hbaseRealTimeTableParam);
    HashMap<String, String> tableRealTime;
    HashMap<String, String> tableNearBy5mParam;
    HBaseOpsImplDao  opsdao = new HBaseOpsImplDao();
    HConnection connect = opsdao.getConnection();
    int  flag  =0 ;
    long totalrequestcount = 0;
    long writerequestcount = 0;
    long readrequestcount = 0;
    
    for(HTableDescriptor table:tables){ 
      String tablename=table.getNameAsString();
       HashMap<String, String> resulttableParam = new HashMap<String, String>() ;
       tableNearBy5mParam=getBefore5mParamByTable(tablename,nowtime,connect,opsdao);
       tableRealTime  = hbaseRealTimeTableParam.get(tablename);     
      if(tableNearBy5mParam.size() !=(tableRealTime.size()+1)){
        // no data  before 5 min
        if(tableNearBy5mParam.size()==0 ){
          flag = 1;
          if(tablename.equals("ns_hbasemonitor:hbase_table_monitor")){
            System.out.println(tablename);
            System.out.println(tableRealTime.get(TableParam.regionNum));
          }
          if(null ==tableRealTime.get(TableParam.regionNum)){
            flag = -1; 
            
            totalrequestcount= -1;
            writerequestcount= -1;    
            readrequestcount=  -1; 
          } else{
            totalrequestcount= Long.parseLong(tableRealTime.get(TableParam.totalRequestCount));
            writerequestcount= Long.parseLong(tableRealTime.get(TableParam.writeRequestCount));    
            readrequestcount=  Long.parseLong(tableRealTime.get(TableParam.readRequestCount));
            near5mtime= clusterstarttime;
           }
        }
        else{//add  disable table and bad tables
          LOG.error(table.getNameAsString()+":tableNearBy5mParam  vaule no equal tableRealTime"); 
          flag = -1; 
        }
                      
      }
      if(flag ==0){
      totalrequestcount= -Long.parseLong(tableNearBy5mParam.get(TableParam.totalRequestCount))+Long.parseLong(tableRealTime.get(TableParam.totalRequestCount));
      writerequestcount= -Long.parseLong(tableNearBy5mParam.get(TableParam.writeRequestCount))+Long.parseLong(tableRealTime.get(TableParam.writeRequestCount));    
      readrequestcount=  -Long.parseLong(tableNearBy5mParam.get(TableParam.readRequestCount))+Long.parseLong(tableRealTime.get(TableParam.readRequestCount));
      long intervaltime = HBaseTimeUtil.subDate(nowtime, near5mtime)/1000;    
      resulttableParam.put(ResultTablesParamStruct.qps,Double.toString( ArithmeticUtil.div(totalrequestcount, intervaltime)));
      resulttableParam.put(ResultTablesParamStruct.wqps,Double.toString( ArithmeticUtil.div(writerequestcount, intervaltime)));
      resulttableParam.put(ResultTablesParamStruct.rqps,Double.toString( ArithmeticUtil.div(readrequestcount, intervaltime)));
     
      resulttableParam.put(ResultTablesParamStruct.regionNum,tableRealTime.get(TableParam.regionNum));
      resulttableParam.put(ResultTablesParamStruct.memsize,tableRealTime.get(TableParam.MemStoreSizeMB));
      resulttableParam.put(ResultTablesParamStruct.filesize,tableRealTime.get(TableParam.storefileSizeMB));
      resulttableParam.put(ResultTablesParamStruct.coprocessor,tableRealTime.get(TableParam.coprocessor));
      LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
      resultTableParam.put(tablename, resulttableParam); 
      flag  =0 ;
      }else if(flag ==-1){
        resulttableParam.put(ResultTablesParamStruct.qps,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.wqps,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.rqps,ErrorFlag.disableTable);      
        resulttableParam.put(ResultTablesParamStruct.regionNum,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.memsize,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.filesize,ErrorFlag.disableTable);
        resulttableParam.put(ResultTablesParamStruct.coprocessor,ErrorFlag.disableTable);
        LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
        resultTableParam.put(tablename, resulttableParam);  
        flag  =0 ;
      }else{
        long intervaltime = HBaseTimeUtil.subDate(nowtime, near5mtime)/1000;    
        resulttableParam.put(ResultTablesParamStruct.qps,Double.toString( ArithmeticUtil.div(totalrequestcount, intervaltime)));
        resulttableParam.put(ResultTablesParamStruct.wqps,Double.toString( ArithmeticUtil.div(writerequestcount, intervaltime)));
        resulttableParam.put(ResultTablesParamStruct.rqps,Double.toString( ArithmeticUtil.div(readrequestcount, intervaltime)));
       
        resulttableParam.put(ResultTablesParamStruct.regionNum,tableRealTime.get(TableParam.regionNum));
        resulttableParam.put(ResultTablesParamStruct.memsize,tableRealTime.get(TableParam.MemStoreSizeMB));
        resulttableParam.put(ResultTablesParamStruct.filesize,tableRealTime.get(TableParam.storefileSizeMB));
        resulttableParam.put(ResultTablesParamStruct.coprocessor,tableRealTime.get(TableParam.coprocessor));
        LOG.debug(table.getNameAsString()+":"+totalrequestcount+","+writerequestcount+","+readrequestcount);
        resultTableParam.put(tablename, resulttableParam); 
        
      }
      
    }
    connect.close();
    return resultTableParam;
   
    
  }



  

  



}
