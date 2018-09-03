import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.log4j.PropertyConfigurator;

import com.suning.hbase.table.monitor.model.RsTablesParamStruct;
import com.suning.hbase.table.monitor.service.RegionServersParam;
import com.suning.hbase.table.monitor.service.TablesParamList;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;


public class TestTablesParamList {
  static {  
    //PropertyHelper.setPropertiesAbsultePath("/home/hbase/workspace/ScheduledCaptureHBaseTableTaram/conf/config.properties");
    PropertyConfigurator.configureAndWatch("/home/hbase/workspace/ScheduledCaptureHBaseTableTaram/conf/log4j.properties", 5000);
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
      HBaseAdmin admin = HBaseUtil.getAdmin();
      HConnection connect = HBaseUtil.getConnection();
      TablesParamList  gtp = new TablesParamList();
      RegionServersParam  rsp = new RegionServersParam();
      RsTablesParamStruct rtps = new RsTablesParamStruct();
      HashMap<String, HashMap<String, String>> tablesParaList = new HashMap<String, HashMap<String, String>>() ;
      HashMap<String, HashMap<String, String>> regionParaList = new HashMap<String, HashMap<String, String>>() ;
 
      try {
      rtps.setTablesParaList( gtp.getTablesParamList(connect));
      rtps.setRegionserverParaList(rsp.getRegionServersParam(admin));
      tablesParaList = rtps.getTablesParaList();
      regionParaList = rtps.getRegionserverParaList();
    } catch (IOException e) {
      
      e.printStackTrace();
    }
    System.out.println(regionParaList.size());
    System.out.println(tablesParaList.size());
    System.out.println("-------------------------------------------------------------");
    Iterator<String> it1 = tablesParaList.keySet().iterator();
    while(it1.hasNext()){
      String key = it1.next();
      System.out.print(key+": ");  
      HashMap<String,String> param = tablesParaList.get(key);
      Iterator<String> i = param.keySet().iterator();
        while(i.hasNext()){
          String key1 = i.next();
          System.out.print("|"+key1); 
          String value1= param.get(key1);
          System.out.print(":"+value1); 
        } 
      System.out.println();
    }
    System.out.println("-------------------------------------------------------------");
    Iterator<String> it = regionParaList.keySet().iterator();
    while(it.hasNext()){
      String key = it.next();
      System.out.print(key+": ");  
      HashMap<String, String> param = regionParaList.get(key);
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
