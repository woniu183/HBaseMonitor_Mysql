package com.suning.hbase.table.monitor.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HConnection;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.table.monitor.model.ClusterTotalRequest;
import com.suning.hbase.util.HBaseUtil;

public class CaptureHBaseClusterParamBaseOnRegion implements Runnable {
	static ClusterTotalRequest clusterparam = new ClusterTotalRequest();
	private static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static void writeClusterParam2HBase() {
		String nowtime = df.format(new Date());
		HConnection connect = null;
		// build connect
		HBaseOpsImplDao opsdao = new HBaseOpsImplDao();
		try {
			connect = opsdao.getConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// capture cluster param
		clusterparam = HBaseUtil.getClusterParam(clusterparam);
		// insert hbase table para
		//opsdao.doInsert(clusterparam, nowtime, connect);
		// close connect

		writeClusterParam2Mysql(nowtime);
		opsdao.close();

	}

	public static void writeClusterParam2Mysql(String nowdate){
		try {
			Properties p=new Properties();
			//InputStream is=ClassLoader.getSystemResourceAsStream("db.properties");
			InputStream is= CaptureHBaseClusterParam.class.getClassLoader().getSystemResourceAsStream("conf/config.properties");
			p.load(is);
			String url =p.getProperty("url");
			String user =p.getProperty("user");
			String password =p.getProperty("password");
			String dbc = url + "?user=" + user + "&password=" + password;
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(dbc);
			Statement statement = connection.createStatement();
			Integer result = statement.executeUpdate("INSERT into cluster_monitor values('" +nowdate + "','" +  clusterparam.getStartTime() + "','" + clusterparam.getClusterRequestCount() + "','" + clusterparam.getClusterReadRequestCount() + "','" + clusterparam.getClusterWriteRequestCount() + "','" + clusterparam.getClusterregionnum() + "','" + clusterparam.getClustermaxheapM() + "','" + clusterparam.getClusteruseheapMb() + "') " );



		}catch (Exception ex){
			ex.printStackTrace();
		}

	}

	@Override
	public void run() {
		writeClusterParam2HBase();
	}

	public static void main(String[] args) {
		writeClusterParam2HBase();
	}
}
