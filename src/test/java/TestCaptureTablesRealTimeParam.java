import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.log4j.PropertyConfigurator;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.table.monitor.service.TablesParamList;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.HBaseUtilWithNoInit;
import com.suning.hbase.util.PropertyHelper;


public class TestCaptureTablesRealTimeParam {
	private static Configuration configuration = HBaseConfiguration.create();
	private static HashMap<String, HashMap<String, String>> hbaseRealTimeTableParam = new HashMap<String, HashMap<String, String>>() ;
  /* static {  
		    PropertyHelper.setPropertiesPath("/home/hbase/workspace/ScheduledCaptureHBaseTableTaram/conf/config.properties");
		    PropertyConfigurator.configureAndWatch("/home/hbase/workspace/ScheduledCaptureHBaseTableTaram/conf/log4j.properties", 5000);
	}*/

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		System.out.println(Long.MAX_VALUE);	
		
		TablesParamList  tpl = new TablesParamList();	
		
		/*configuration.set("hbase.zookeeper.quorum", "namenode1-prd2.cnsuning.com,namenode2-prd2.cnsuning.com,resourcemgr1-prd2.cnsuning.com,resourcemgr2-prd2.cnsuning.com,slave1-prd2.cnsuning.com");
    	configuration.set("hbase.zookeeper.property.clientPort", "2015");
    	configuration.set("zookeeper.znode.parent", "/hbasen1"); */  
			
		 configuration.addResource(new Path("hbase-site.xml"));	
		  HBaseAdmin admin = new HBaseAdmin(configuration);
		  Configuration conf = admin.getConfiguration();
		 String regionmax = conf.get("hbase.hregion.max.filesize");
		 
	     int rsize = Integer.parseInt(regionmax.substring(0, regionmax.length()-9));	 
		 System.out.println(rsize);
		 
		 ArrayList<HTableDescriptor> tables = HBaseUtilWithNoInit.getAllTable(configuration); 
		 
		 for(HTableDescriptor table:tables){
			 System.out.println(table.getNameAsString());			 
		 }
		 
		 hbaseRealTimeTableParam= tpl.CaptureTablesRealTimeParam(tables , hbaseRealTimeTableParam,configuration);
		 
	  	 System.out.println(hbaseRealTimeTableParam.size());
		
		   Iterator<String> it1 = hbaseRealTimeTableParam.keySet().iterator();
		   
		    while(it1.hasNext()){
		      String key = it1.next();
		      System.out.print(key+": ");  
		      HashMap<String,String> param = hbaseRealTimeTableParam.get(key);
		      Iterator<String> i = param.keySet().iterator();
		        while(i.hasNext()){
		          String key1 = i.next();
		          System.out.print("|"+key1); 
		          String value1= param.get(key1);
		          System.out.print(":"+value1); 
		        } 
		      System.out.println();
		    }

	}

}
