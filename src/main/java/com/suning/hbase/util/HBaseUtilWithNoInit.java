package com.suning.hbase.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.model.ClusterTotalQps;
import com.suning.hbase.table.monitor.model.ClusterTotalRequest;
import com.suning.hbase.table.monitor.model.ErrorFlag;
import com.suning.hbase.table.monitor.model.RegionFileSize;
import com.suning.hbase.table.monitor.model.RegionServerParam;
import com.suning.hbase.table.monitor.model.TableParam;

public class HBaseUtilWithNoInit {

  private static Logger LOG = LoggerFactory.getLogger(HBaseUtilWithNoInit.class);
  private static ByteBuffer buffer = ByteBuffer.allocate(8); 
  private static ThreadLocal<HConnection> tlconnection = new ThreadLocal<HConnection>();
  private static Configuration configuration = HBaseConfiguration.create();
  private static String zookeeperQuorum;
  private static String port = "2181";
  private static String znodeparent;
  private static String hbasetable;
  private static String  clustertable;
  private static HConnection  conn = null;

  public static void setZookeeperQuorum(String zq, String port, String znodeparent) {
    configuration.set("hbase.zookeeper.quorum", zq);
    configuration.set("hbase.zookeeper.property.clientPort", port);
    configuration.set("zookeeper.znode.parent", znodeparent);
  }

  public void setZookeeperQuorum(String zq, String port) {
    configuration.set("hbase.zookeeper.quorum", zq);
    configuration.set("hbase.zookeeper.property.clientPort", port);

  }

  public void setZookeeperQuorum(String zq) {
    configuration.set("hbase.zookeeper.quorum", zq);
  }

  public void setHBaseTable(String tablename){
    
    hbasetable=tablename;
  }
  
  
  public static String  getHBaseTable(){
    if(null == hbasetable){
      init();   
    }
    return hbasetable;    
  }
  public void setClusterTable(String tablename){
    
    clustertable=tablename;
  }
  
  
  public static String  getClusterTable(){
    if(null == clustertable){
      init();   
    }
    return clustertable;    
  }
  public static void init() {
    PropertyHelper.setPropertiesPath("conf/config.properties");
    zookeeperQuorum = PropertyHelper.getKeyValue("hbase.zookeeper.quorum");
    if (null == zookeeperQuorum) {
      LOG.error("hbase.zookeeper.quorum  not config in conf/config.properties");
      ////System.exit(-1);
    }
    port = PropertyHelper.getKeyValue("hbase.zookeeper.property.clientPort");
    // need to check the path exits or not in zk
    znodeparent = PropertyHelper.getKeyValue("zookeeper.znode.parent");
    
    hbasetable = PropertyHelper.getKeyValue("tablename");
    clustertable=PropertyHelper.getKeyValue("clustermonitortable");
    setZookeeperQuorum(zookeeperQuorum, port, znodeparent);
  }

  public static ArrayList<HTableDescriptor> getAllTable(HConnection  conn) {
    ArrayList<HTableDescriptor> list = new ArrayList<HTableDescriptor>();
    try {
      
     /* HBaseAdmin admin = new HBaseAdmin(configuration);   
      HTableDescriptor[] tables = admin.listTables();*/
      HTableDescriptor[] tables= conn.listTables();
      for (HTableDescriptor table : tables) {
        if(!table.equals("hbase:acl" )&& !table.equals("hbase:meta") && !table.equals("hbase:namespace")){
          list.add(table);         
        }       
      }

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }
  @SuppressWarnings("resource")
  public static ArrayList<HTableDescriptor> getAllTable() {
    ArrayList<HTableDescriptor> list = new ArrayList<HTableDescriptor>();
    try {
      HBaseAdmin admin = new HBaseAdmin(configuration);   
      HTableDescriptor[] tables = admin.listTables();
      for (HTableDescriptor table : tables) {
        if(!table.equals("hbase:acl" )&& !table.equals("hbase:meta") && !table.equals("hbase:namespace")){
          list.add(table);         
        }       
      }

    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }
  
  public static ArrayList<HTableDescriptor> getAllTable( Configuration conf) {
	    ArrayList<HTableDescriptor> list = new ArrayList<HTableDescriptor>();
	    try {
	    	configuration= conf;
	      HBaseAdmin admin = new HBaseAdmin(configuration);   
	      HTableDescriptor[] tables = admin.listTables();
	      for (HTableDescriptor table : tables) {
	        if(!table.equals("hbase:acl" )&& !table.equals("hbase:meta") && !table.equals("hbase:namespace")){
	          list.add(table);         
	        }       
	      }

	    } catch (MasterNotRunningException e) {
	      e.printStackTrace();
	    } catch (ZooKeeperConnectionException e) {
	      e.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	    return list;
	  }

  @SuppressWarnings("resource")
  public static String getClusterStartTime(){
    ClusterStatus cs = null;
    Date date = null;
    HBaseAdmin admin;
    try {
       admin = new HBaseAdmin(configuration);
       cs = admin.getClusterStatus();
       date = new Date(cs.getMaster().getStartcode());
    } catch (IOException e) {
      
      e.printStackTrace();
    }
   
    return  HBaseTimeUtil.sdf.format(date);    
  }
  public static Long bytesToLong(byte[] bytes) {    
      buffer.put(bytes, 0, bytes.length);  
      buffer.flip();//need flip   
      return buffer.getLong();     
}  
  
  public static void getTablesCoprocessor(ArrayList<HTableDescriptor>tables,HashMap<String, HashMap<String, String>> hbaseTableParam){
    HashMap<String, String> tableParamMap;
    String tablename ;
    for (HTableDescriptor table : tables) {   
      tablename= table.getNameAsString();
      if(!tablename.equals("hbase:acl" )&& !tablename.equals("hbase:meta") && !tablename.equals("hbase:namespace")){
      tableParamMap =new HashMap<String, String>();      
      tableParamMap.put(TableParam.coprocessor, String.valueOf(table.getCoprocessors().size()));
      hbaseTableParam.put(table.getNameAsString(), tableParamMap);      
      LOG.debug(table.getNameAsString() + ":" + table.getCoprocessors().size());
      }else{
        tables.remove(table);
      }     
    }
  }
  
  public static HBaseAdmin  getAdmin(){
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(configuration);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return admin;
  }
  public static  HashMap<String, HashMap<String, String>>  getRegionServerParam(){
    HashMap<String, HashMap<String, String>> regionServerParam = new HashMap<String, HashMap<String, String>>();
   
    ServerLoad load ;
    int maxheapMb  = -1;
    Date starttime  = new Date() ;
    int regionnum  = -1;
    int useheapMb  = -1;   
    long readrequestscount = -1;
    long writerequestscount  = -1;
    long totalnumberrequest  = -1;
    try {
      
      HBaseAdmin admin = new HBaseAdmin(configuration);
      ClusterStatus cs = admin.getClusterStatus();
      Collection<ServerName> rss = cs.getServers();
      ServerName master = cs.getMaster();
      
      Collection<ServerName> backmaster = cs.getBackupMasters();
      Collection<ServerName> deadrss = cs.getDeadServerNames();
     // add regionserver to map
     for(ServerName rs:rss){
       HashMap<String, String>  serverParam = new   HashMap<String, String>();
        load = cs.getLoad(rs);
        starttime=new Date(rs.getStartcode());
        regionnum= load.getNumberOfRegions();
        maxheapMb = load.getMaxHeapMB();
        useheapMb = load.getUsedHeapMB();
        readrequestscount=load.getReadRequestsCount();
        writerequestscount= load.getWriteRequestsCount();
        totalnumberrequest=load.getTotalNumberOfRequests();  
        serverParam.put(RegionServerParam.starttime, starttime.toString());
        serverParam.put(RegionServerParam.regionnum,String.valueOf(regionnum));
        serverParam.put(RegionServerParam.useheapMb,String.valueOf(useheapMb));
        serverParam.put(RegionServerParam.maxheapMb,String.valueOf(maxheapMb));
        serverParam.put(RegionServerParam.readrequestscount,String.valueOf(readrequestscount));
        serverParam.put(RegionServerParam.writerequestscount,String.valueOf(writerequestscount));
        serverParam.put(RegionServerParam.totalnumberrequest,String.valueOf(totalnumberrequest));
        regionServerParam.put(rs.getServerName(),serverParam);
       // serverParam.clear();
     }
     
     //add deadregionserver to map
     
     for(ServerName rs:deadrss){
       HashMap<String, String>  serverParam = new   HashMap<String, String>();
       serverParam.put(RegionServerParam.starttime, ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.regionnum, ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.useheapMb,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.maxheapMb,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.readrequestscount,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.writerequestscount,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.totalnumberrequest,ErrorFlag.deadregionserver);
       regionServerParam.put(rs.getServerName(),serverParam);
      // serverParam.clear();   
     }
     
   
    
       
    } catch (IOException e) {
      
      e.printStackTrace();
    }    
    return regionServerParam;   
  }
  public static  HashMap<String, HashMap<String, String>>  getRegionServerParam( HBaseAdmin admin){
    HashMap<String, HashMap<String, String>> regionServerParam = new HashMap<String, HashMap<String, String>>();
   
    ServerLoad load ;
    int maxheapMb  = -1;
    Date starttime  = new Date() ;
    int regionnum  = -1;
    int useheapMb  = -1;   
    long readrequestscount = -1;
    long writerequestscount  = -1;
    long totalnumberrequest  = -1;
    try {
   
      ClusterStatus cs = admin.getClusterStatus();
      Collection<ServerName> rss = cs.getServers();
      
      Collection<ServerName> backmaster = cs.getBackupMasters();
      Collection<ServerName> deadrss = cs.getDeadServerNames();
     // add regionserver to map
     for(ServerName rs:rss){
       HashMap<String, String>  serverParam = new   HashMap<String, String>();
        load = cs.getLoad(rs);
        starttime=new Date(rs.getStartcode());
        regionnum= load.getNumberOfRegions();
        maxheapMb = load.getMaxHeapMB();
        useheapMb = load.getUsedHeapMB();
        
        for (RegionLoad region : load.getRegionsLoad().values()) {
          readrequestscount+=region.getReadRequestsCount();
          writerequestscount+= region.getWriteRequestsCount();
          totalnumberrequest+=region.getRequestsCount();
        }
        totalnumberrequest=readrequestscount+writerequestscount;
        serverParam.put(RegionServerParam.starttime, starttime.toString());
        serverParam.put(RegionServerParam.regionnum,String.valueOf(regionnum));
        serverParam.put(RegionServerParam.useheapMb,String.valueOf(useheapMb));
        serverParam.put(RegionServerParam.maxheapMb,String.valueOf(maxheapMb));
        serverParam.put(RegionServerParam.readrequestscount,String.valueOf(readrequestscount));
        serverParam.put(RegionServerParam.writerequestscount,String.valueOf(writerequestscount));
        serverParam.put(RegionServerParam.totalnumberrequest,String.valueOf(totalnumberrequest));
        regionServerParam.put(rs.getServerName(),serverParam);
       // serverParam.clear();
     }
     
     //add deadregionserver to map
     
     for(ServerName rs:deadrss){
       HashMap<String, String>  serverParam = new   HashMap<String, String>();
       serverParam.put(RegionServerParam.starttime, ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.regionnum, ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.useheapMb,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.maxheapMb,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.readrequestscount,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.writerequestscount,ErrorFlag.deadregionserver);
       serverParam.put(RegionServerParam.totalnumberrequest,ErrorFlag.deadregionserver);
       regionServerParam.put(rs.getServerName(),serverParam);
      // serverParam.clear();   
     }
     
   
    
       
    } catch (IOException e) {
      
      e.printStackTrace();
    }    
    return regionServerParam;   
  }
  
  
  public static List<RegionFileSize> getRegionFileSizeDistribution(){
    List<RegionFileSize> list = new ArrayList<RegionFileSize>();
      
    try {
      @SuppressWarnings("resource")
      HBaseAdmin admin = new HBaseAdmin(configuration);
      ClusterStatus cs = admin.getClusterStatus();
      Collection<ServerName> rss = cs.getServers();
      for(ServerName rs:rss){   
        ServerLoad load = cs.getLoad(rs);
        Map<byte[], RegionLoad> regionsload = load.getRegionsLoad();
        Iterator<byte[]> iter = regionsload.keySet().iterator();
        while(iter.hasNext()){
          RegionFileSize rfz = new RegionFileSize();
          byte[] key = iter.next();
          RegionLoad regionload = regionsload.get(key);
          String regionname = new String(key);
          rfz.setRegionname(regionname);
          rfz.setFilenum(regionload.getStorefiles());
          rfz.setFilesize(regionload.getStorefileSizeMB());    
        //  LOG.info(regionname+":"+regionload.getStorefiles()+"|"+regionload.getStorefileSizeMB());
          list.add(rfz);
        }        
        
      }
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }
  public static  ClusterTotalRequest  getClusterParam(ClusterTotalRequest ctqps){ 
    ServerLoad load ;
    Date starttime  = new Date() ; 
    long clusterrequestcount = 0;
    long clusterreadrequestcount = 0;
    long clusterwriterequestcount = 0;
    long clusterregionnum  =0;
    long clustermaxheapMb  =0;
    long clusteruseheapMb  =0;
  
    try {
      
      @SuppressWarnings("resource")
      HBaseAdmin admin = new HBaseAdmin(configuration);
      ClusterStatus cs = admin.getClusterStatus();
     ServerName master = cs.getMaster();
     new Date(master.getStartcode());
      Collection<ServerName> rss = cs.getServers();      
     for(ServerName rs:rss){
       starttime=new Date(rs.getStartcode());
        load = cs.getLoad(rs);
        for (RegionLoad region : load.getRegionsLoad().values()) {
          clusterreadrequestcount+=region.getReadRequestsCount();
          clusterwriterequestcount+=region.getWriteRequestsCount();
          clusterrequestcount+=region.getRequestsCount();        
        }     
        clusterregionnum += load.getNumberOfRegions();
        clustermaxheapMb += load.getMaxHeapMB();
        clusteruseheapMb += load.getUsedHeapMB();       
     }
     ctqps.setStartTime(starttime.toString()); 
     ctqps.setClusterWriteRequestCount(clusterwriterequestcount);
     ctqps.setClusterReadRequestCount(clusterreadrequestcount);
     ctqps.setClusterRequestCount(clusterrequestcount);
     ctqps.setClusterregionnum(clusterregionnum);
     ctqps.setClustermaxheapM(clustermaxheapMb);
     ctqps.setClusteruseheapMb(clusteruseheapMb);   
    } catch (IOException e) {
      
      e.printStackTrace();
    }    
    return ctqps;   
  }
  
  
  
  public static ClusterTotalQps getTablesParam(HashMap<String, HashMap<String, String>> hbaseTableParam,Configuration conf) {
	    ClusterStatus cs;
	    String[] regionname;
	    String table;
	    HashMap<String, String> tableParamMap =new HashMap<String, String>() ;
	    long totalRequestCount;
	    try {   
	     configuration =conf;
		  HBaseAdmin admin = new HBaseAdmin(configuration);
	      cs = admin.getClusterStatus();
	      Collection<ServerName> rss = cs.getServers();
	      for (ServerName rs : rss) {
	        ServerLoad load = cs.getLoad(rs);
	        
	        LOG.debug("table:"+" "+"RequestsCount"+" "+"ReadRequestsCount" +" "+"WriteRequestsCount" +" "+"StorefileSizeMB"+" "+"MemStoreSizeMB");
	        for (RegionLoad region : load.getRegionsLoad().values()) {
	          regionname = region.getNameAsString().split(",");
	          table = regionname[0];
	          if(table.equals("ns_hbasemonitor:hbase_table_monitor")){
	            System.out.println("ns_hbasemonitor:hbase_table_monitor");            
	          }
	        
	          if(!hbaseTableParam.containsKey(table)){
	            
	            hbaseTableParam.put(table,tableParamMap);
	          }
	          tableParamMap = hbaseTableParam.get(table);

	         if(tableParamMap.containsKey(TableParam.totalRequestCount)){
	            totalRequestCount =region.getRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.totalRequestCount));
	         }else{
	            totalRequestCount =region.getRequestsCount();
	         }
	     
	          tableParamMap.put(TableParam.totalRequestCount, String.valueOf(totalRequestCount));
	          
	          if(tableParamMap.containsKey(TableParam.writeRequestCount)){
	            tableParamMap.put( TableParam.writeRequestCount,String.valueOf(region.getWriteRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.writeRequestCount))));
	          }else{
	            tableParamMap.put( TableParam.writeRequestCount,String.valueOf(region.getWriteRequestsCount()));          
	          }
	            
	          if(tableParamMap.containsKey(TableParam.readRequestCount)){
	            tableParamMap.put( TableParam.readRequestCount,String.valueOf(region.getReadRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.readRequestCount))));
	          }else{
	            tableParamMap.put( TableParam.readRequestCount,String.valueOf(region.getReadRequestsCount()));          
	          }
	            
	          
	          if(tableParamMap.containsKey(TableParam.storefileSizeMB)){
	            tableParamMap.put( TableParam.storefileSizeMB,String.valueOf(region.getStorefileSizeMB()+ Integer.parseInt(tableParamMap.get(TableParam.storefileSizeMB))));
	          }else{
	            tableParamMap.put( TableParam.storefileSizeMB,String.valueOf(region.getStorefileSizeMB()));          
	          }
	                  

	          if(tableParamMap.containsKey(TableParam.MemStoreSizeMB)){
	            tableParamMap.put( TableParam.MemStoreSizeMB,String.valueOf(region.getMemStoreSizeMB()+ Integer.parseInt(tableParamMap.get(TableParam.MemStoreSizeMB))));
	            if(Integer.parseInt(tableParamMap.get(TableParam.MemStoreSizeMB))>0){
	             // System.out.println(region.getNameAsString()+":aaaaa"+region.getMemStoreSizeMB());
	            }
	          }else{
	            tableParamMap.put( TableParam.MemStoreSizeMB,String.valueOf(region.getMemStoreSizeMB()));          
	          }
	          
	         
	          if(tableParamMap.containsKey(TableParam.regionNum)){
	            tableParamMap.put( TableParam.regionNum,String.valueOf(1 + Integer.parseInt(tableParamMap.get(TableParam.regionNum))));
	          }else{
	            tableParamMap.put( TableParam.regionNum,String.valueOf(1));          
	          }
	              

	          hbaseTableParam.put(table, tableParamMap);
	         
	         /*  LOG.debug(region.getNameAsString() + ":" 
	             +region.getRequestsCount()+ " "
	             + region.getReadRequestsCount() + " "
	             + region.getWriteRequestsCount() + " "
	             +region.getStorefileSizeMB()+  " "
	             + region.getMemStoreSizeMB());
	  */
	          
	         /* System.out.println(region.getNameAsString() + ":" 
	              +region.getRequestsCount()+ " "
	              + region.getReadRequestsCount() + " "
	              + region.getWriteRequestsCount() + " "
	              +region.getStorefileSizeMB()+  " "
	              + region.getMemStoreSizeMB());*/
	   
	        }
	      }
	    } catch (IOException e) {
	      
	      e.printStackTrace();
	    }
	    System.out.println(hbaseTableParam.size());
	    return null;

	  }

  
  
  public static ClusterTotalQps getTablesParam(HashMap<String, HashMap<String, String>> hbaseTableParam) {
    ClusterStatus cs;
    String[] regionname;
    String table;
    HashMap<String, String> tableParamMap =new HashMap<String, String>() ;
    long totalRequestCount;
    try {
      HBaseAdmin admin = new HBaseAdmin(configuration);
      cs = admin.getClusterStatus();
      Collection<ServerName> rss = cs.getServers();
      for (ServerName rs : rss) {
        ServerLoad load = cs.getLoad(rs);
        
        LOG.debug("table:"+" "+"RequestsCount"+" "+"ReadRequestsCount" +" "+"WriteRequestsCount" +" "+"StorefileSizeMB"+" "+"MemStoreSizeMB");
        for (RegionLoad region : load.getRegionsLoad().values()) {
          regionname = region.getNameAsString().split(",");
          table = regionname[0];
          if(table.equals("ns_hbasemonitor:hbase_table_monitor")){
            System.out.println("ns_hbasemonitor:hbase_table_monitor");            
          }
        
          if(!hbaseTableParam.containsKey(table)){
            
            hbaseTableParam.put(table,tableParamMap);
          }
          tableParamMap = hbaseTableParam.get(table);

         if(tableParamMap.containsKey(TableParam.totalRequestCount)){
            totalRequestCount =region.getRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.totalRequestCount));
         }else{
            totalRequestCount =region.getRequestsCount();
         }
     
          tableParamMap.put(TableParam.totalRequestCount, String.valueOf(totalRequestCount));
          
          if(tableParamMap.containsKey(TableParam.writeRequestCount)){
            tableParamMap.put( TableParam.writeRequestCount,String.valueOf(region.getWriteRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.writeRequestCount))));
          }else{
            tableParamMap.put( TableParam.writeRequestCount,String.valueOf(region.getWriteRequestsCount()));          
          }
            
          if(tableParamMap.containsKey(TableParam.readRequestCount)){
            tableParamMap.put( TableParam.readRequestCount,String.valueOf(region.getReadRequestsCount()+ Integer.parseInt(tableParamMap.get(TableParam.readRequestCount))));
          }else{
            tableParamMap.put( TableParam.readRequestCount,String.valueOf(region.getReadRequestsCount()));          
          }
            
          
          if(tableParamMap.containsKey(TableParam.storefileSizeMB)){
            tableParamMap.put( TableParam.storefileSizeMB,String.valueOf(region.getStorefileSizeMB()+ Integer.parseInt(tableParamMap.get(TableParam.storefileSizeMB))));
          }else{
            tableParamMap.put( TableParam.storefileSizeMB,String.valueOf(region.getStorefileSizeMB()));          
          }
                  

          if(tableParamMap.containsKey(TableParam.MemStoreSizeMB)){
            tableParamMap.put( TableParam.MemStoreSizeMB,String.valueOf(region.getMemStoreSizeMB()+ Integer.parseInt(tableParamMap.get(TableParam.MemStoreSizeMB))));
            if(Integer.parseInt(tableParamMap.get(TableParam.MemStoreSizeMB))>0){
             // System.out.println(region.getNameAsString()+":aaaaa"+region.getMemStoreSizeMB());
            }
          }else{
            tableParamMap.put( TableParam.MemStoreSizeMB,String.valueOf(region.getMemStoreSizeMB()));          
          }
          
         
          if(tableParamMap.containsKey(TableParam.regionNum)){
            tableParamMap.put( TableParam.regionNum,String.valueOf(1 + Integer.parseInt(tableParamMap.get(TableParam.regionNum))));
          }else{
            tableParamMap.put( TableParam.regionNum,String.valueOf(1));          
          }
              

          hbaseTableParam.put(table, tableParamMap);
          
         /*  LOG.debug(region.getNameAsString() + ":" 
             +region.getRequestsCount()+ " "
             + region.getReadRequestsCount() + " "
             + region.getWriteRequestsCount() + " "
             +region.getStorefileSizeMB()+  " "
             + region.getMemStoreSizeMB());
  */
          
         /* System.out.println(region.getNameAsString() + ":" 
              +region.getRequestsCount()+ " "
              + region.getReadRequestsCount() + " "
              + region.getWriteRequestsCount() + " "
              +region.getStorefileSizeMB()+  " "
              + region.getMemStoreSizeMB());*/
   
        }
      }
    } catch (IOException e) {
      
      e.printStackTrace();
    }
    return null;

  }

  public static HConnection getConnection() {    
       try {
        conn= HConnectionManager.createConnection(configuration);
      } catch (IOException e) {
        e.printStackTrace();
      }    
    return conn;
  }



  public static void main(String[] args) {
    getAllTable();
  }
}
