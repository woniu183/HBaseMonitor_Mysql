package com.suning.hbase.table.monitor.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.dao.HBaseOpsImplDao;
import com.suning.hbase.table.monitor.model.ClusterTotalQps;
import com.suning.hbase.table.monitor.model.ClusterfamilyStruct;
import com.suning.hbase.util.ArithmeticUtil;
import com.suning.hbase.util.HBaseTimeUtil;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;

public class ClusterParam {
  static {  
    PropertyHelper.setPropertiesPath("conf/config.properties");
    PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
}
  private static Logger LOG = LoggerFactory.getLogger(ClusterParam.class);
  private static   DecimalFormat df = new DecimalFormat("#.####"); 

  public static void main(String[] args) {
    if(args.length <3){
      LOG.info("usage:java(version7) -jar  *jar filename '2014-03-31 13:00:00' '2015-04-31 13:00:00'");
      System.exit(-1);
    }
    String head =
            "<head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"></head>";
    String startstring =
        "<br><br><h2 align = \"center\">HBase集群指标统计</h2><table border=\"1\" style=\"width:100%; margin:auto auto\">\n";
    String endstring =
        "<table border=\"1\" align = \"center\" style=\"width:100%; margin:auto auto\">\n";
    String  htmlheader= "<tr> "
    +"<th>Time</th>"
    +"<th>QPS</th>"
    +"<th>WriteQPS</th>" 
    +"<th>ReadQPS</th>"
    +"<th>UseHeapMB</th>"
    +"<th>MaxHeapMB</th>"
    +"<th>RegionNum</th>"
    +"</tr>"
    +"\n";
   
    File file = new File(args[0]);
    try (FileOutputStream fop = new FileOutputStream(file)) {
      if (!file.exists()) {
        file.createNewFile();
       }
       fop.write(head.getBytes());
      fop.write(startstring.getBytes());
      fop.write(htmlheader.getBytes());
      //String endtime =HBaseTimeUtil.sdf.format(new Date());
      //List<ClusterTotalQps> ctqlist = getetClusterParamByTimeRange(args[1],args[2]);
        List<ClusterTotalQps> ctqlist =getetClusterParamByTimeRangeFromMysql(args[1],args[2]);
      for(ClusterTotalQps ctq:ctqlist){
        fop.write("<tr> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(ctq.getdate().getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(Double.toString(ctq.getqps()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(Double.toString(ctq.getwqps()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(Double.toString(ctq.getrqps()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(String.valueOf(ctq.getClusteruseheapMb()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(String.valueOf(ctq.getClustermaxheapM()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(String.valueOf(ctq.getClusterregionnum()).getBytes());
        fop.write("</td> ".getBytes());
        fop.write("</tr> ".getBytes());
        fop.write("\n".getBytes());
      }
      fop.write(endstring.getBytes());
      
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

    public static List<ClusterTotalQps> getetClusterParamByTimeRangeFromMysql(String starttime,String endtime){
        List<ClusterTotalQps> ctqlist = new ArrayList<ClusterTotalQps>();

        Long intervaltime  = (long) 300;
        long cmptotalrequest =0;
        long cmpwriterequest =0;
        long cmpreadrequest  =0;
        String cmptime = null;
        String time = null;

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

            ResultSet result =statement.executeQuery("SELECT * FROM cluster_monitor WHERE date BETWEEN '"+starttime+"' AND '"+endtime+"'");
            int count = 0;
            while (result.next()){
                ClusterTotalQps  ctq = new ClusterTotalQps();
                if(count==0){
                    time =result.getString("date");
                    ctq.setdate(time);
                    String starttime1 =result.getString("starttime");
                    SimpleDateFormat sdf1= new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

                    SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String starttime2=sdf2.format(sdf1.parse(starttime1));
                    intervaltime = HBaseTimeUtil.subDate(time,starttime2)/1000;
                    cmptotalrequest  = Long.parseLong(result.getString("clusterrequestcount"));
                    ctq.setqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,intervaltime))));
                    cmpwriterequest  = Long.parseLong(result.getString("clusterwriterequestcount"));
                    ctq.setwqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmpwriterequest,intervaltime))));
                    cmpreadrequest  = Long.parseLong(result.getString("clusterreadrequestcount"));
                    ctq.setrqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmpreadrequest,intervaltime))));
                    ctq.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));
                    ctq.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));
                    ctq.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));


                }else {
                    time =result.getString("date");
                    ctq.setdate(time);


                    intervaltime = HBaseTimeUtil.subDate(time,cmptime)/1000;

                    double value = ArithmeticUtil.sub(result.getString("clusterrequestcount"),cmptotalrequest);
                    ctq.setqps( Double.parseDouble(df.format(ArithmeticUtil.div(value,intervaltime))));
                    cmptotalrequest  = Long.parseLong(result.getString("clusterrequestcount"));

                    double wvalue = ArithmeticUtil.sub(result.getString("clusterwriterequestcount"),cmpwriterequest);
                    ctq.setwqps( Double.parseDouble(df.format(ArithmeticUtil.div(wvalue,intervaltime))));
                    cmpwriterequest  = Long.parseLong(result.getString("clusterwriterequestcount"));

                    double rvalue = ArithmeticUtil.sub(result.getString("clusterreadrequestcount"),cmpreadrequest);
                    ctq.setrqps( Double.parseDouble(df.format(ArithmeticUtil.div(rvalue,intervaltime))));
                    cmpreadrequest  = Long.parseLong(result.getString("clusterreadrequestcount"));

                    ctq.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));
                    ctq.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));
                    ctq.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));




                }
                count +=1;
                cmptime  = time;
                ctqlist.add(ctq);

                // String ClusterRequestCount =result.getString("clusterrequestcount");
                // System.out.println(ClusterRequestCount);


            }
            //Integer result = statement.executeUpdate("INSERT into cluster_monitor values('" +nowdate + "','" +  clusterparam.getStartTime() + "','" + clusterparam.getClusterRequestCount() + "','" + clusterparam.getClusterReadRequestCount() + "','" + clusterparam.getClusterWriteRequestCount() + "','" + clusterparam.getClusterregionnum() + "','" + clusterparam.getClustermaxheapM() + "','" + clusterparam.getClusteruseheapMb() + "') " );


        }catch (Exception ex) {
            ex.printStackTrace();
        }

        return ctqlist;
    }


}

