package com.suning.hbase.table.monitor.dao;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

public class HBaseTableDemo {
  static {
    PropertyConfigurator.configure("log4j.properties");
  }
  private static Logger LOG = LoggerFactory.getLogger(HBaseTableDemo.class);

  public static void main(String[] args) throws InterruptedException, ServiceException,
      KeeperException {
    /*
     * if(args.length != 2){ LOG.info("singleInsertData.jar   tableName count"); return ; } String
     * tablename = args[0]; int count = Integer.parseInt(args[1]);
     */
    int count = 1000000;
    Configuration configuration = HBaseConfiguration.create();
    // configuration.addResource(".//src//main//resources//hbase-site.xml");
    /*LOG.info("hbase.zookeeper.quorum",
      "sup01-pre.cnsuning.com,sup02-pre.cnsuning.com,sup03-pre.cnsuning.com");
    configuration.set("hbase.zookeeper.quorum",
      "sup01-pre.cnsuning.com,sup02-pre.cnsuning.com,sup03-pre.cnsuning.com");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("zookeeper.znode.parent", "/hbase1");*/
    configuration.addResource("conf/hbase-site.xml");
    // configuration.set("hbase.zookeeper.quorum","slave1-dev.cnsuning.com,slave2-dev.cnsuning.com,slave3-dev.cnsuning.com");
    // configuration.set("hbase.zookeeper.property.clientPort", "2222");
    // configuration.set("zookeeper.znode.parent", "/home/hbase/snhbase");
    String tableName = args[0];
    //getTablesReadWriteCount(configuration, tableName);
    bulkData(configuration, tableName, 1);

  }

  /**
   * create a new Table
   * @param configuration Configuration
   * @param tableName String,the new Table's name
   */
  public static void createNameSpace(Configuration configuration, String tableName) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(configuration);

      admin.createNamespace(NamespaceDescriptor.create("lap").build());

      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor("info"));
      tableDescriptor.addFamily(new HColumnDescriptor("address"));
      admin.createTable(tableDescriptor);
      LOG.info("end create table");
      admin.close();
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      
      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }

  }

  /**
   * create a new Table
   * @param configuration Configuration
   * @param tableName String,the new Table's name
   */
  public static void createTable(Configuration configuration, String tableName) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(configuration);

      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        LOG.info(tableName + "is exist ,delete ..............................................");
      }

      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor("info"));
      tableDescriptor.addFamily(new HColumnDescriptor("address"));
      admin.createTable(tableDescriptor);
      LOG.info("end create table");
      admin.close();
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      
      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }

  }

  /**
   * Delete the existing table
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  public static void dropTable(Configuration configuration, String tableName) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(configuration);
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "delete success!");
      } else {
        System.out.println(tableName + "Table does not exist!");
      }
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      
      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }

  /**
   * insert a data
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  public static void addData(Configuration configuration, String tableName) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(configuration);
      if (admin.tableExists(tableName)) {
        HTable table = new HTable(configuration, tableName);
        Put put = new Put(Bytes.toBytes("zhangsan"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("29"));
        put.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("29"));
        table.put(put);
        System.out.println("add success!");
      } else {
        System.out.println(tableName + "Table does not exist!");
      }
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      
      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }

  public static String getRandomString(int length) { // length表示生成字符串的长度
    String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(base.length());
      sb.append(base.charAt(number));
    }
    return sb.toString();
  }

  public static void getTablesReadWriteCount(Configuration configuration, String tableName)
      throws IOException {
    @SuppressWarnings("resource")
    HBaseAdmin admin = new HBaseAdmin(configuration);
    ClusterStatus cs = admin.getClusterStatus();
    Collection<ServerName> rss = cs.getServers();
    for (ServerName rs : rss) {
      ServerLoad load = cs.getLoad(rs);
      System.out.println(load.getRsCoprocessors().length);

      for (RegionLoad region : load.getRegionsLoad().values()) {

        /*
         * System.out.println(region.getNameAsString() + ":" + region.getReadRequestsCount() + " " +
         * region.getWriteRequestsCount() + " " + region.getMemStoreSizeMB() + " " +
         * region.getStorefileSizeMB());
         */
        region.getRequestsCount();
      }
    }

  }
  public static void getRegionLocations(Configuration configuration, String tableName, int count)
      throws InterruptedException, ServiceException, IOException, KeeperException {
    HTable table = new HTable(configuration, tableName);

    HBaseAdmin admin = new HBaseAdmin(configuration);

    Map<ServerName, Integer> regDistribution = new TreeMap<ServerName, Integer>();
      Map<HRegionInfo, ServerName> regions = table.getRegionLocations();
    if (regions != null && regions.size() > 0) {
      for (Map.Entry<HRegionInfo, ServerName> hriEntry : regions.entrySet()) {
        HRegionInfo regionInfo = hriEntry.getKey();
        ServerName addr = hriEntry.getValue();
        long req = 0;
        float locality = 0.0f;
        String urlRegionServer = null;
        ClusterStatus cs = admin.getClusterStatus();
        cs.getMaster();
        Collection<ServerName> rss = cs.getServers();
        for (ServerName rs : rss) {
          ServerLoad load = cs.getLoad(rs);
          load.getRegionsLoad();
          for (RegionLoad region : load.getRegionsLoad().values()) {
            System.out.println(region.getNameAsString() + ":" + region.getReadRequestsCount() + " "
                + region.getWriteRequestsCount());
          }

          /*
           * ServerLoad rcs = cs.getLoad(rs); // System.out.println("load:" + rcs. ()); int rqc =
           * rcs.getReadRequestsCount(); int wqc = rcs.getWriteRequestsCount();
           * System.out.println(rs.getHostname() + ":" + rqc + " " + wqc);
           */
        }

        /*
         * System.out.println(master.getServerManager().getOnlineServers()); if (addr != null) {
         * ServerLoad sl = master.getServerManager().getLoad(addr); if (sl != null) { Map<byte[],
         * RegionLoad> map = sl.getRegionsLoad(); if (map.containsKey(regionInfo.getRegionName())) {
         * req = map.get(regionInfo.getRegionName()).getRequestsCount(); locality =
         * map.get(regionInfo.getRegionName()).getDataLocality(); } Integer i =
         * regDistribution.get(addr); if (null == i) i = Integer.valueOf(0);
         * regDistribution.put(addr, i + 1); } }
         */
      }
    }
    }
  /**
   * insert a data
   * @param configuration Configuration
   * @param tableName String,Table's name
   * @throws InterruptedException
   * @throws ServiceException
   */
  @SuppressWarnings("deprecation")
  public static void bulkData(Configuration configuration, String tableName, int count)
      throws InterruptedException, ServiceException {
    HBaseAdmin admin;

    try {
      admin = new HBaseAdmin(configuration);
      Configuration conf = admin.getConfiguration();
      Iterator<Entry<String, String>> it = conf.iterator();
      while(it.hasNext()){
        Entry<String, String> itt = it.next();
        System.out.println(itt.getKey()+":"+itt.getValue());
      }
        
      if (!admin.tableExists(tableName)) {
        LOG.info("table :" + tableName + "is not exists");
      }
      // admin.balancer();
      @SuppressWarnings("resource")
      HTable table = new HTable(configuration, tableName);
      Map<HRegionInfo, ServerName> regions = table.getRegionLocations(); 
      table.setAutoFlush(true, true);
      int writeBuffer = 1024 * 1024;
      if (writeBuffer != 0) {
        table.setWriteBufferSize(writeBuffer);
      }

      List<Put> pl = new ArrayList<Put>();
      long begin = System.currentTimeMillis();
      int count1 = 0;
     String  randomstring= getRandomString(1024);
      for (int i = 0; i < count; i++) {
        Put put = new Put(Bytes.toBytes(getRandomString(5)));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(randomstring));
        pl.add(put);
        if (i % 100 == 0) {
          table.put(pl);
          table.flushCommits();
          Thread.sleep(100);
          LOG.info("insert " + i);
        }
        table.put(pl);
        table.flushCommits();
      }

      long end = System.currentTimeMillis();
      LOG.info("ops is :" + (1000 * count / (end - begin)));

    } catch (RetriesExhaustedWithDetailsException e) {
  
      e.printStackTrace();
    } catch (MasterNotRunningException e) {
   
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      
      e.printStackTrace();
    } catch (InterruptedIOException e) {
      
      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }

  /**
   * insert a data
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  @SuppressWarnings("deprecation")
  public static void singleInsertData(Configuration configuration, String tableName, int count) {
    HBaseAdmin admin;

    try {
      admin = new HBaseAdmin(configuration);
      if (!admin.tableExists(tableName)) {
        LOG.info("table :" + tableName + "is not exists");
      }

      HTable table = new HTable(configuration, tableName);
      table.setAutoFlush(true, true);
      /*
       * // int writeBuffer = 1024*1024; table.setWriteBufferSize(writeBuffer);
       */
      HashMap<Integer, String> map = new HashMap();

      for (int i = 0; i < count; i++) {
        map.put(i, getRandomString(10));
      }
      long begin = System.currentTimeMillis();
      Iterator<Entry<Integer, String>> iter = map.entrySet().iterator();
      int i = 0;
      while (iter.hasNext()) {
        Map.Entry<Integer, String> info = iter.next();
        Put put = new Put(Bytes.toBytes(info.getValue()));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("29"));
        table.put(put);
        i++;
        table.flushCommits();
        if (i % 10000 == 0) {
          LOG.info("singleInsertData " + i);
          LOG.info("singleInsertData ops:" + i * 1000 / (System.currentTimeMillis() - begin));
        }
      }
      long end = System.currentTimeMillis();
      LOG.info("ops is :" + (1000 * count / (end - begin)));

    } catch (RetriesExhaustedWithDetailsException e) {
      
      e.printStackTrace();
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
 
      e.printStackTrace();
    } catch (InterruptedIOException e) {
      e.printStackTrace();
    } catch (IOException e) {
    
      e.printStackTrace();
    }
  }

  /**
   * Delete a data
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  public static void deleteDate(Configuration configuration, String tableName) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(configuration);
      if (admin.tableExists(tableName)) {
        HTable table = new HTable(configuration, tableName);
        Delete delete = new Delete(Bytes.toBytes("zhangsan"));
        table.delete(delete);
        System.out.println("delete success!");
      } else {
        System.out.println("Table does not exist!");
      }
    } catch (MasterNotRunningException e) {
      
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {

      e.printStackTrace();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }

  /**
   * get a data
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  public static void getData(Configuration configuration, String tableName) {
    HTable table = null;

    try {
      HBaseAdmin admin = new HBaseAdmin(configuration);
      if (!admin.tableExists(tableName)) {

      }
      table = new HTable(configuration, tableName);
      Get get = new Get(Bytes.toBytes("zhangsan"));
      Result result = table.get(get);
      admin.close();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
  }

  @SuppressWarnings("resource")
  public static void singleSelect(Configuration configuration, String tableName) throws Exception {
    HTable table = null;
    HBaseAdmin admin = new HBaseAdmin(configuration);
    try {
      if (!admin.tableExists(tableName)) {

        throw new Exception("table noexistes");
      }
      table = new HTable(configuration, tableName);
      Get get = new Get(Bytes.toBytes("zhangsan"));
      Result result = table.get(get);

      for (Cell cell : result.rawCells()) {
        LOG.info("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
        LOG.info("Timetamp:" + cell.getTimestamp() + " ");
        LOG.info("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
        LOG.info("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
        LOG.info("value:" + new String(CellUtil.cloneValue(cell)) + " ");
      }
      admin.close();
    } catch (IOException e) {
      admin.close();
      table.close();
      e.printStackTrace();
    }
  }

  /**
   * insert all data
   * @param configuration Configuration
   * @param tableName String,Table's name
   */
  @SuppressWarnings("deprecation")
  public static void getAllData(Configuration configuration, String tableName) {
    HTable table;
    Scan scan = new Scan();
    try {
      table = new HTable(configuration, tableName);
      ResultScanner results = table.getScanner(scan);
      int count = 0;
      for (Result result : results) {
        // System.out.println(result);
        List<KeyValue> res = result.getColumn("info".getBytes(), "age2".getBytes());
        //result.getValue(family, qualifier);

        Iterator<KeyValue> it = res.iterator();
        while (it.hasNext()) {
          KeyValue itt = it.next();
          // System.out.println(itt);
        }


      }
    } catch (IOException e) {
      
      e.printStackTrace();
    }

  }

  public static void batchSelect(Configuration configuration, String tableName) throws IOException {
    HTable table = null;
    Scan scan = new Scan();
    try {
      table = new HTable(configuration, tableName);

    } catch (IOException e) {
      
      e.printStackTrace();
    } finally {
      table.close();
    }

  }
}
