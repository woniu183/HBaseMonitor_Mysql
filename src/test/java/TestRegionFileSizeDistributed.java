import org.apache.log4j.PropertyConfigurator;

import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;


public class TestRegionFileSizeDistributed {
  static {  
    PropertyHelper.setPropertiesPath("conf/config.properties");
    PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
  }
  
  public static void main(String[] args) {    
    HBaseUtil.getRegionFileSizeDistribution();

  }

}
