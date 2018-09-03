import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.mortbay.log.Log;

import com.suning.hbase.table.monitor.model.TableQps;
import com.suning.hbase.table.monitor.model.TableResultParamStuct;
import com.suning.hbase.table.monitor.model.TablefsizeMsize;
import com.suning.hbase.table.monitor.service.TableParamByTimeRange;
import com.suning.hbase.util.HBaseUtil;


public class TestTableParamByTimeRange implements Runnable  {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    
    /*for(int i = 0;i < 1; i++){
      new Thread(new TestTableParamByTimeRange()).start();  
      
    }*/
    
   if(args.length<3){
     
     Log.info("useage:java -jar *jar tablename '2015-04-03 06:33:00' '2015-04-03 07:10:00' ");
     
     
   }
    HConnection connect = HBaseUtil.getConnection();
    TableParamByTimeRange   tp = new TableParamByTimeRange();
    TableResultParamStuct trps = tp.getetTableParamByTimeRange(args[0], args[1], args[2],connect);
    List<TableQps> plists = trps.getTableQpsList();
    List<TablefsizeMsize> fmlists = trps.getTablefsizeMsizeList();
    trps.getTablefsizeMsizeList();
    for(TableQps list:plists){
      System.out.println(list.getdate()+":"+list.getqps()+"|"+list.getrqps()+"|"+list.getwqps());
    }
    
    for(TablefsizeMsize fm:fmlists){
      System.out.println(fm.getdate()+":"+fm.getfsize()+""+fm.getmsize()); 
    }
    System.out.println(fmlists.size());
    //System.out.println(plists.size());
    
  
  }

  @Override
  public void run() {
    HConnection connect = HBaseUtil.getConnection();
    TableParamByTimeRange   tp = new TableParamByTimeRange();
    TableResultParamStuct trps = tp.getetTableParamByTimeRange("ns_aps:tb_unionlog_date", "2015-04-03 06:33:00", "2015-04-08 07:10:00",connect);
    List<TableQps> plists = trps.getTableQpsList();
    List<TablefsizeMsize> fmlists = trps.getTablefsizeMsizeList();
    trps.getTablefsizeMsizeList();
    for(TableQps list:plists){
      System.out.println(list.getdate()+":"+list.getqps()+"|"+list.getrqps()+"|"+list.getwqps());
    }
    
    for(TablefsizeMsize fm:fmlists){
      System.out.println(fm.getdate()+":"+fm.getfsize()+""+fm.getmsize()); 
    }
    System.out.println(fmlists.size());
    //System.out.println(plists.size());
    
  }
 

}
