package com.suning.hbase.table.monitor.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;

public class CaptureHBaseTableParam implements Runnable {
    static {
        PropertyHelper.setPropertiesPath("conf/config.properties");
        PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
    }

    private static HashMap<String, HashMap<String, String>> hbaseTableParam =
            new HashMap<String, HashMap<String, String>>();
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Logger LOG = LoggerFactory.getLogger(CaptureHBaseTableParam.class);

    public static void writeHbaseParam2Mysql() {
        try {

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String nowtime = df.format(new Date());

            Properties p = new Properties();
            //InputStream is=ClassLoader.getSystemResourceAsStream("db.properties");
            InputStream is = CaptureHBaseClusterParam.class.getClassLoader().getSystemResourceAsStream("conf/config.properties");
            p.load(is);
            String url = p.getProperty("url");
            String user = p.getProperty("user");
            String password = p.getProperty("password");
            String dbc = url + "?user=" + user + "&password=" + password;
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection(dbc);
            Statement statement = connection.createStatement();

            for (Map.Entry<String, HashMap<String, String>> me : hbaseTableParam.entrySet()) {//me是存放hashmap中取出的内容，并用Map.Entry<> 指定其泛型
                String tablename = me.getKey();
                HashMap<String, String> hm = me.getValue();
                String MemStoreSizeMB = hm.get("MemStoreSizeMB");
                String storefileSizeMB = hm.get("storefileSizeMB");
                String totalRequestCount = hm.get("totalRequestCount");
                String writeRequestCount = hm.get("writeRequestCount");
                String readRequestCount = hm.get("readRequestCount");
                String regionNum = hm.get("regionNum");
                String coprocessor = hm.get("coprocessor");
                Integer result = statement.executeUpdate("INSERT into hbase_table_monitor values('" + nowtime + "','" + tablename + "','" + MemStoreSizeMB + "','" + storefileSizeMB + "','" + totalRequestCount + "','" + writeRequestCount + "','" + readRequestCount + "','" + regionNum + "','" + coprocessor + "') " +
                        "ON DUPLICATE KEY UPDATE tablename='" + tablename + "',date='" + nowtime + "'");
                // System.out.println(result);
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void main(String[] args) {
        ArrayList<HTableDescriptor> tables = HBaseUtil.getAllTable();
        for (HTableDescriptor table : tables) {
            System.out.println(table.getNameAsString());
        }
        HBaseUtil.getTablesCoprocessor(tables, hbaseTableParam);
        HBaseUtil.getTablesParam(hbaseTableParam);
        writeHbaseParam2Mysql();
        // writeHbaseTableParam2HBase(hbaseTableParam);
    }

    public void run() {
        ArrayList<HTableDescriptor> tables = HBaseUtil.getAllTable();
        LOG.info("get tables list and table count is:" + tables.size());
        HBaseUtil.getTablesCoprocessor(tables, hbaseTableParam);
        HBaseUtil.getTablesParam(hbaseTableParam);
        writeHbaseParam2Mysql();
        // writeHbaseTableParam2HBase(hbaseTableParam);
    }

}




