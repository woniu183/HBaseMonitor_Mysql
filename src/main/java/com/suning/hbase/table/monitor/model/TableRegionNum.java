package com.suning.hbase.table.monitor.model;

public class TableRegionNum {
  private int regionnum ;
  private String date;
  public void setregionnum(int value){
    regionnum=value;   
  }
  public long getregionnum(){
   return regionnum;  
  }
  
  public void setdate(String value){
    date=value;   
  }
  
  public String getdate(){
   return date;   
  }
 

}
