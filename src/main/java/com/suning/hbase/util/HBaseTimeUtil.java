package com.suning.hbase.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



public class HBaseTimeUtil {
  public static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
  
  public static String subDate5Min(String time){ 
    Date date = null;
    try {
      date = sdf.parse(time);
    } catch (ParseException e) {
      
      e.printStackTrace();
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, -5);
    return sdf.format(cal.getTime());    
  }
  
  public static  String subDate(String time,int minute){ 
    Date date = null;
    try {
      date = sdf.parse(time);
    } catch (ParseException e) {
      
      e.printStackTrace();
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MINUTE, minute);
    return sdf.format(cal.getTime());    
  }
  
  public static  Long subDate(String time1,String time2){ 
    Date date1 = null;
    Date date2 = null;
    try {
      date1 = sdf.parse(time1);
      date2 = sdf.parse(time2);
    } catch (ParseException e) {
     
      e.printStackTrace();
    }
    Calendar c1 = Calendar.getInstance();
    Calendar c2 = Calendar.getInstance();
    c1.setTime(date1);
    c2.setTime(date2);
    long mills =
        c1.getTimeInMillis() > c2.getTimeInMillis()
         ? c1.getTimeInMillis() - c2.getTimeInMillis()
         : c2.getTimeInMillis() - c1.getTimeInMillis();
    return mills;   
  }
  
  
  public static void main(String[] args) {
    
    System.out.println(subDate5Min("2015-11-16 12:10:12"));
     System.out.println(subDate("2017-11-16 12:10:12",1000));
     System.out.println(subDate("2015-11-16 12:10:12","2017-11-16 12:10:12"));
  
   
  }
}
