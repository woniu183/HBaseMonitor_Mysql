package com.suning.hbase.table.monitor.service;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.table.monitor.model.TableQps;
import com.suning.hbase.table.monitor.model.TableRegionNum;
import com.suning.hbase.table.monitor.model.TableResultParamStuct;
import com.suning.hbase.table.monitor.model.TablefsizeMsize;
import com.suning.hbase.util.ArithmeticUtil;
import com.suning.hbase.util.HBaseTimeUtil;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;

public class TableParamByTimeRange{
     static {  
        //   PropertyHelper.setPropertiesPath("conf/config.properties");
           PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
     }
     private static   DecimalFormat df = new DecimalFormat("###########0.0"); 
    private static Logger LOG = LoggerFactory.getLogger(TableParamByTimeRange.class);
    public  TableResultParamStuct  getetTableParamByTimeRange(String tablename,String starttime,String endtime,HConnection connection ){
      TableResultParamStuct tsps = new TableResultParamStuct();
      List<TableQps> qpslist = new ArrayList<TableQps>();
      List<TablefsizeMsize> fmlist = new ArrayList<TablefsizeMsize>();
      List<TableRegionNum> rnlist = new ArrayList<TableRegionNum>();
      HBaseOpsImplDao hod= new HBaseOpsImplDao();
    
      Long intervaltime  = (long) 300;
      String clusterstarttime = HBaseUtil.getClusterStartTime();
      int count = 0;
      if(connection.isClosed()){
        connection=HBaseUtil.getConnection();
        hod.setConnection(connection);
      }
      TablesParamList  tp = new TablesParamList();    
    // HashMap<String, String> begindata = tp.getBefore5mParamByTable(tablename, starttime, connection, hod); 
      ResultScanner results = hod.doSelectByTimeRange(tablename, starttime, endtime, connection);
      String  date =null;
      String  time  =null;
      String cmptotalrequest =null;
      String cmpwriterequest =null;
      String cmpreadrequest  =null;
      
      for (Result result : results) { 
        TableQps tmptableqps = new  TableQps();
        TablefsizeMsize  tmptfm= new TablefsizeMsize();
        TableRegionNum tmprn = new TableRegionNum();
        for(Cell cell:result.rawCells()){
           time = new String(CellUtil.cloneRow(cell)).substring(tablename.length());
         tmptableqps.setdate(time);
         tmptfm.setdate(time);
         tmprn.setdate(time);
         
          if(count == 0){       
            intervaltime = HBaseTimeUtil.subDate(time,clusterstarttime)/1000;           
            switch(new String(CellUtil.cloneQualifier(cell))){    
            case "rqc":
                  cmptotalrequest  = new String(CellUtil.cloneValue(cell));
                  double rqcvalue = Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,  String.valueOf(intervaltime))));
                  if(rqcvalue<0){
                    rqcvalue =Math.abs(rqcvalue);
                  }
                  tmptableqps.setqps(rqcvalue);
                  break;
            case  "wrqc":
                   cmpwriterequest  = new String(CellUtil.cloneValue(cell));
                   double wrqcvalue = Double.parseDouble(df.format(ArithmeticUtil.div(cmpwriterequest,  String.valueOf(intervaltime))));
                   if(wrqcvalue<0){
                     wrqcvalue =Math.abs(wrqcvalue);
                   }
                   tmptableqps.setwqps(wrqcvalue);
                   break;
            case  "rrqc":
                   cmpreadrequest  = new String(CellUtil.cloneValue(cell));
                   double rrqcvalue = Double.parseDouble(df.format(ArithmeticUtil.div(cmpreadrequest,  String.valueOf(intervaltime))));
                   if(rrqcvalue<0){
                     rrqcvalue =Math.abs(rrqcvalue);
                   }
                   tmptableqps.setrqps(rrqcvalue);
                  break;
            case  "rnum": 
                   tmprn.setregionnum(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "fsize":  
                    tmptfm.setfsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "msize":   
                   tmptfm.setmsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "cp":
                   LOG.debug("coprocessor not need in table graphics");
                   break;
            default:
                LOG.error("ERROR match "+new String(CellUtil.cloneQualifier(cell)));     
            }
          }else{
            intervaltime = HBaseTimeUtil.subDate(time, date.toString())/1000;
            switch(new String(CellUtil.cloneQualifier(cell))){    
            case "rqc":
                 double value = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)),cmptotalrequest);
                 cmptotalrequest  = new String(CellUtil.cloneValue(cell));
                 if(value<0){
                   value = Math.abs(value);
                 }     
                 tmptableqps.setqps(Double.parseDouble(df.format(ArithmeticUtil.div(value,  String.valueOf(intervaltime)))));
                  break;
            case  "wrqc":
                 double wvalue = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)), cmpwriterequest);
                  if(wvalue<0){
                    wvalue = Math.abs(wvalue);
                   } 
                  cmpwriterequest  = new String(CellUtil.cloneValue(cell));
                  tmptableqps.setwqps(Double.parseDouble(df.format(ArithmeticUtil.div(wvalue,  String.valueOf(intervaltime)))));
                   break;
            case  "rrqc":
                  double rvalue = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)),cmpreadrequest);
                  if(rvalue<0){
                    rvalue = Math.abs(rvalue);
                  } 
                  cmpreadrequest  = new String(CellUtil.cloneValue(cell));
                  tmptableqps.setrqps(Double.parseDouble(df.format(ArithmeticUtil.div(rvalue,  String.valueOf(intervaltime)))));         
                  break;
            case  "rnum": 
                   tmprn.setregionnum(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "fsize":  
                    tmptfm.setfsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "msize":   
                   tmptfm.setmsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "cp":
                   LOG.debug("coprocessor not need in table graphics");
                   break;
            default:
                LOG.error("ERROR match "+new String(CellUtil.cloneQualifier(cell)));     
            }       
          }
         
         /* System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");        
          System.out.println("Timetamp:"+cell.getTimestamp()+" ");
          System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
          System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
          System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");*/
          
        }
        count ++;
        date = time;      
        qpslist.add(tmptableqps);
        fmlist.add(tmptfm);
        rnlist.add(tmprn);
      }
      tsps.setTableQpsList(qpslist); 
      tsps.setTablefsizeMsizeList(fmlist);
      tsps.setTableRegionNumList(rnlist);
      return tsps;
      
    }
    public  TableResultParamStuct  getetTableParamByTimeRange(String tablename,String starttime,String endtime){
    TableResultParamStuct tsps = new TableResultParamStuct();
    List<TableQps> qpslist = new ArrayList<TableQps>();
    List<TablefsizeMsize> fmlist = new ArrayList<TablefsizeMsize>();
    List<TableRegionNum> rnlist = new ArrayList<TableRegionNum>();
    HBaseOpsImplDao hod= new HBaseOpsImplDao();
    HConnection connection = null  ;
    Long intervaltime  = (long) 300;
    String clusterstarttime = HBaseUtil.getClusterStartTime();
    int count = 0;
    try {
      connection = hod.getConnection();
      if(connection.isClosed()){
        connection=HBaseUtil.getConnection();
        hod.setConnection(connection);
      }
      TablesParamList  tp = new TablesParamList();    
     // HashMap<String, String> begindata = tp.getBefore5mParamByTable(tablename, starttime, connection, hod); 
      ResultScanner results = hod.doSelectByTimeRange(tablename, starttime, endtime, connection);
      String  date =null;
      String  time  =null;
      String cmptotalrequest =null;
      String cmpwriterequest =null;
      String cmpreadrequest  =null;
      
      for (Result result : results) { 
        TableQps tmptableqps = new  TableQps();
        TablefsizeMsize  tmptfm= new TablefsizeMsize();
        TableRegionNum tmprn = new TableRegionNum();
        for(Cell cell:result.rawCells()){
           time = new String(CellUtil.cloneRow(cell)).substring(tablename.length());
         tmptableqps.setdate(time);
         tmptfm.setdate(time);
         tmprn.setdate(time);
         
        /* System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
         System.out.println("Timetamp:"+cell.getTimestamp()+" ");
         System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
         System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
         System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");*/
         
          if(count == 0){       
            intervaltime = HBaseTimeUtil.subDate(time,clusterstarttime)/1000;           
            switch(new String(CellUtil.cloneQualifier(cell))){    
            case "rqc":
                  cmptotalrequest  = new String(CellUtil.cloneValue(cell));
                  Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,  String.valueOf(intervaltime))));
                  tmptableqps.setqps(Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,  String.valueOf(intervaltime)))));
                  break;
            case  "wrqc":
                   cmpwriterequest  = new String(CellUtil.cloneValue(cell));
                   tmptableqps.setwqps(Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,  String.valueOf(intervaltime)))));
                   break;
            case  "rrqc":
                   cmpreadrequest  = new String(CellUtil.cloneValue(cell));
                   tmptableqps.setrqps(Double.parseDouble(df.format(ArithmeticUtil.div(cmpreadrequest,  String.valueOf(intervaltime)))));
                  break;
            case  "rnum": 
                   tmprn.setregionnum(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "fsize":  
                    tmptfm.setfsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "msize":   
                   tmptfm.setmsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "cp":
                   LOG.debug("coprocessor not need in table graphics");
                   break;
            default:
                LOG.error("ERROR match "+new String(CellUtil.cloneQualifier(cell)));     
            }
          }else{
            intervaltime = HBaseTimeUtil.subDate(time, date.toString())/1000;
            switch(new String(CellUtil.cloneQualifier(cell))){    
            case "rqc":
                 double value = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)),cmptotalrequest);
                 cmptotalrequest  = new String(CellUtil.cloneValue(cell));
                 String test=df.format(ArithmeticUtil.div(value,  String.valueOf(intervaltime)));
                 tmptableqps.setqps(Double.parseDouble(df.format(ArithmeticUtil.div(value,  String.valueOf(intervaltime)))));
                  break;
            case  "wrqc":
                 double wvalue = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)), cmpwriterequest);
                  cmpwriterequest  = new String(CellUtil.cloneValue(cell));
                  tmptableqps.setwqps(Double.parseDouble(df.format(ArithmeticUtil.div(wvalue,  String.valueOf(intervaltime)))));
                   break;
            case  "rrqc":
                  double rvalue = ArithmeticUtil.sub(new String(CellUtil.cloneValue(cell)),cmpreadrequest);
                  cmpreadrequest  = new String(CellUtil.cloneValue(cell));
                  tmptableqps.setrqps(Double.parseDouble(df.format(ArithmeticUtil.div(rvalue,  String.valueOf(intervaltime)))));         
                  break;
            case  "rnum": 
                   tmprn.setregionnum(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "fsize":  
                    tmptfm.setfsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "msize":   
                   tmptfm.setmsize(Integer.parseInt(new String(CellUtil.cloneValue(cell))));
                   break;
            case  "cp":
                   LOG.debug("coprocessor not need in table graphics");
                   break;
            default:
                LOG.error("ERROR match "+new String(CellUtil.cloneQualifier(cell)));     
            }       
          }
         
         /* System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");        
          System.out.println("Timetamp:"+cell.getTimestamp()+" ");
          System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
          System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
          System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");*/
          
        }
        count ++;
        date = time;      
        qpslist.add(tmptableqps);
        fmlist.add(tmptfm);
        rnlist.add(tmprn);
      }
     
      // convert result to list 
      
      //compute qps  and   get fsize ,msize,region num;
      
      connection.close();
    } catch (IOException e) {
      e.printStackTrace();
   }
    tsps.setTableQpsList(qpslist); 
    tsps.setTablefsizeMsizeList(fmlist);
    tsps.setTableRegionNumList(rnlist);
    return tsps;
    
  }

}
