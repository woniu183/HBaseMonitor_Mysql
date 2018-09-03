package com.suning.hbase.table.monitor.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.suning.hbase.table.monitor.model.RegionFileSize;
import com.suning.hbase.util.ArithmeticUtil;
import com.suning.hbase.util.HBaseUtil;
import com.suning.hbase.util.PropertyHelper;

public class RegionFileSizeDistribution {
  private static Logger LOG = LoggerFactory.getLogger(RegionFileSizeDistribution.class);
  private static Long range;
  private  static Long max;
  private  static Long tatolnum;

  static {  
    PropertyHelper.setPropertiesPath("conf/config.properties");
    PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
  }
  public  static long [] getRangeArray(List<RegionFileSize> list,long range){
    long [] rangearray =new long[10];
    for(RegionFileSize rfs:list){
      long filesize = rfs.getFilesize();
      
      if(filesize<range){
        rangearray[0]++;
     
      }
      else if(filesize<2*range){
        rangearray[1]++;
       
      }
      else if(filesize<3*range){
        rangearray[2]++;
       
      }
      else if(filesize<4*range){
        rangearray[3]++;
        
      }
      else if(filesize<5*range){
        rangearray[4]++;
       
      }
      else if(filesize<6*range){
        rangearray[5]++;
       
      }
      else if(filesize<7*range){
        rangearray[6]++;
     
      }
      else if(filesize<8*range){
        rangearray[7]++;
       
      }
      else if(filesize<9*range){
        rangearray[8]++;
       
      }
      else{
        rangearray[9]++;
        
      }
         
     LOG.info(rfs.getRegionname()+":"+rfs.getFilesize()+"|"+rfs.getFilenum());
      
    }
    return rangearray;
    
  }
  public static  void write2file(File file,String startstring,String endstring,String htmlheader,long [] rangearray){
    try (FileOutputStream fop = new FileOutputStream(file)) {
      if (!file.exists()) {
        file.createNewFile();
       }
        
      fop.write(startstring.getBytes());
      fop.write(htmlheader.getBytes());
      
      for(int i =0 ; i < rangearray.length;i++){
        fop.write("<tr> ".getBytes());
        fop.write("<td>".getBytes());
        fop.write((i*range+"-"+(i+1)*range+"(M)").getBytes());
        fop.write("</td> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());
        fop.write((""+rangearray[i]).getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("<td  style=\"text-align:right\"> ".getBytes());      
        fop.write((""+ArithmeticUtil.mul2(ArithmeticUtil.div(rangearray[i], tatolnum, 4), 100, 2)+"%").getBytes());
        fop.write("</td  style=\"text-align:right\"> ".getBytes());
        fop.write("</tr> ".getBytes());
      }
      fop.write(endstring.getBytes());
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
  public static void main(String[] args) { 
    if(args.length <1){
      LOG.info("usage:java(version7) -jar  *jar filename");
      System.exit(-1);
    }
    
   List<RegionFileSize> list= HBaseUtil.getRegionFileSizeDistribution();
   Collections.sort(list);
 
   
   
   for(RegionFileSize rfs:list){
       max = rfs.getFilesize();      
       break;
   }
   max = HBaseUtil.getConfRegionMax()*1024;
      tatolnum=(long)0;
   for(RegionFileSize rfs:list){

     tatolnum +=1;
 }
 
    range = max/10;
    
   long [] rangearray= getRangeArray( list, range);
    for(int i =0 ;i<10;i++){    
      LOG.info(i*range+"-"+(i+1)*range+":"+rangearray[i]);      
    }
    
    String startstring =
        "<br><br><h2 align = \"center\"> Region Size Distribution</h2><table border=\"1\" style=\"width:100%; margin:auto auto\">\n";
    String endstring =
        "<table border=\"1\" align = \"center\" style=\"width:100%; margin:auto auto\">\n";
    String  htmlheader= "<tr> "
    +"<th>Range</th>"
    +"<th>Count</th>"
    +"<th>percent</th>"
    +"</tr>"
    +"\n";
   
     File file = new File(args[0]);
   // File file = new File("conf/rfsd.html");
    write2file(file,startstring,endstring,htmlheader,rangearray);
  }

  
  

}
