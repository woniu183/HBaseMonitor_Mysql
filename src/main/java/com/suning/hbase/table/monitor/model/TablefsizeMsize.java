package com.suning.hbase.table.monitor.model;

public class TablefsizeMsize {
  private String date;
  private int fsize ;
  private int msize;

  
  public void setfsize(int value){
    fsize=value;   
  }
  public long getfsize(){
   return fsize;  
  }
  
  public void setmsize(int value){
    msize=value;   
  }
  public long getmsize(){
   return msize;  
  }
  public void setdate(String value){
    date=value;   
  }
  
  public String getdate(){
   return date;   
  }
  

}
