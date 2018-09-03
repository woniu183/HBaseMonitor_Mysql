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
import java.util.*;

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
import com.suning.hbase.table.monitor.model.ClusterTotalRequest;
import com.suning.hbase.table.monitor.model.ClusterfamilyStruct;
import com.suning.hbase.util.ArithmeticUtil;
import com.suning.hbase.util.HBaseTimeUtil;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;

public class DailyReportClusterParam {
  static {  
    PropertyHelper.setPropertiesPath("conf/config.properties");
    PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
  }
  private static Logger LOG = LoggerFactory.getLogger(DailyReportClusterParam.class);
  private static DecimalFormat df = new DecimalFormat("###########0.00");
  private static ClusterTotalRequest first = new ClusterTotalRequest();
  private static ClusterTotalRequest last =new ClusterTotalRequest();

  public static void main(String[] args) {
    if(args.length <1){
      LOG.info("usage:java(version7) -jar  *jar filename ");
      System.exit(-1);
    }
   

    String startstring =
        "<br><br><h2 align = \"center\">HBase cluster statistics</h2><table border=\"1\" style=\"width:100%; margin:auto auto\">\n";
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
      fop.write(startstring.getBytes());
      fop.write(htmlheader.getBytes());
      //String endtime =HBaseTimeUtil.sdf.format(new Date());
      String nowtime = HBaseTimeUtil.sdf.format(new Date());
      String style="style=\"text-align:right\"";
        //List<ClusterTotalQps> ctqlist = getetClusterParamByTimeRange(HBaseTimeUtil.subDate(nowtime, -1440), nowtime);
        List<ClusterTotalQps> ctqlist = getetClusterParamByTimeRangeFromMysql(HBaseTimeUtil.subDate(nowtime, -1440), nowtime);
        int count = 0;
        for(ClusterTotalQps ctq:ctqlist){
        count ++;
        fop.write("<tr> ".getBytes());
        fop.write("<td> ".getBytes());
        fop.write(ctq.getdate().getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(df.format(ctq.getqps()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(df.format(ctq.getwqps()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(df.format(ctq.getrqps()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(String.valueOf(ctq.getClusteruseheapMb()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(String.valueOf(ctq.getClustermaxheapM()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write(String.valueOf(ctq.getClusterregionnum()).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("</tr> ".getBytes());
        fop.write("\n".getBytes());
      }
      long interval = HBaseTimeUtil.subDate(first.getStartTime(),last.getStartTime())/1000;
      
      long writerequestcount = Math.abs(-first.getClusterWriteRequestCount()+last.getClusterWriteRequestCount());
      long readrequestcount = Math.abs(-first.getClusterReadRequestCount()+last.getClusterReadRequestCount());
      long requestcount= writerequestcount+readrequestcount;
      long regionnum = -first.getClusterregionnum()+last.getClusterregionnum();
      long useheapmb = -first.getClusteruseheapMb()+last.getClusteruseheapMb();
      long maxheapmb= -first.getClustermaxheapM()+last.getClustermaxheapM();
      fop.write("<tr> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write("Total Increment".getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(df.format(ArithmeticUtil.div(requestcount, interval)).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(Double.toString(ArithmeticUtil.div(writerequestcount, interval)).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(Double.toString(ArithmeticUtil.div(readrequestcount, interval)).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(Long.toString(useheapmb).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(Long.toString(maxheapmb).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("<td  style=\"text-align:right\"> ".getBytes());
      fop.write(Long.toString(regionnum).getBytes());
      fop.write("</td  style=\"text-align:right\"> ".getBytes());
      fop.write("</tr> ".getBytes());
      fop.write(endstring.getBytes()); 
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

  public  static List<ClusterTotalQps> getetClusterParamByTimeRangeFromMysql(String starttime,String endtime) {

        List<ClusterTotalQps> ctqlist = new ArrayList<ClusterTotalQps>();

        Long intervaltime  = (long) 300;
        long cmptotalrequest =0;
        long cmpwriterequest =0;
        long cmpreadrequest  =0;
        String cmptime = null;
        String time = null;

        try {
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

            ResultSet result = statement.executeQuery("SELECT * FROM cluster_monitor WHERE date BETWEEN '" + starttime + "' AND '" + endtime + "'");
            int count = 0;
            while (result.next()){
                ClusterTotalQps  ctq = new ClusterTotalQps();
                time =result.getString("date");
                ctq.setdate(time);


                if(count==0){
                    first.setStartTime(time);

                    String starttime1 =result.getString("starttime");
                    SimpleDateFormat sdf1= new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
                    SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String starttime2=sdf2.format(sdf1.parse(starttime1));
                    intervaltime = HBaseTimeUtil.subDate(time,starttime2)/1000;

                    cmptotalrequest  = Long.parseLong(result.getString("clusterrequestcount"));
                    ctq.setqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmptotalrequest,intervaltime))));
                    first.setClusterRequestCount(cmptotalrequest);

                    cmpwriterequest  = Long.parseLong(result.getString("clusterwriterequestcount"));
                    ctq.setwqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmpwriterequest,intervaltime))));
                    first.setClusterWriteRequestCount(cmpwriterequest);

                    cmpreadrequest  = Long.parseLong(result.getString("clusterreadrequestcount"));
                    ctq.setrqps( Double.parseDouble(df.format(ArithmeticUtil.div(cmpreadrequest,intervaltime))));
                    first.setClusterReadRequestCount(cmpreadrequest);


                    ctq.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));
                    first.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));

                    ctq.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));
                    first.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));

                    ctq.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));
                    first.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));


                }else {
                    time =result.getString("date");
                    last.setStartTime(time);

                    intervaltime = HBaseTimeUtil.subDate(time,cmptime)/1000;

                    double value = ArithmeticUtil.sub(result.getString("clusterrequestcount"),cmptotalrequest);
                    ctq.setqps( Double.parseDouble(df.format(ArithmeticUtil.div(value,intervaltime))));
                    cmptotalrequest  = Long.parseLong(result.getString("clusterrequestcount"));
                    last.setClusterRequestCount(cmptotalrequest);

                    double wvalue = ArithmeticUtil.sub(result.getString("clusterwriterequestcount"),cmpwriterequest);
                    ctq.setwqps( Double.parseDouble(df.format(ArithmeticUtil.div(wvalue,intervaltime))));
                    cmpwriterequest  = Long.parseLong(result.getString("clusterwriterequestcount"));
                    last.setClusterWriteRequestCount(cmpwriterequest);

                    double rvalue = ArithmeticUtil.sub(result.getString("clusterreadrequestcount"),cmpreadrequest);
                    ctq.setrqps( Double.parseDouble(df.format(ArithmeticUtil.div(rvalue,intervaltime))));
                    cmpreadrequest  = Long.parseLong(result.getString("clusterreadrequestcount"));
                    last.setClusterReadRequestCount(cmpreadrequest);

                    ctq.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));
                    last.setClusterregionnum(Long.parseLong(result.getString("clusterregionnum")));

                    ctq.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));
                    last.setClusteruseheapMb(Long.parseLong(result.getString("clusteruseheapMb")));


                    ctq.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));
                    last.setClustermaxheapM(Long.parseLong(result.getString("clustermaxheapMb")));


                }
                count +=1;
                cmptime  = time;
                ctqlist.add(ctq);

                // String ClusterRequestCount =result.getString("clusterrequestcount");
                // System.out.println(ClusterRequestCount);


            }


        }catch (Exception ex){
            ex.printStackTrace();
        }

        return  ctqlist;



    }




}
