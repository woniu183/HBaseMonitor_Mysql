package com.suning.hbase.table.monitor.model;

public class TableQps {
  private String date; 
  private double   qps ;
  private double   wqps;
  private double  rqps;
  
  public void setqps(double d){
    qps=d;   
  }
  public double getqps(){
   return qps;  
  }
  
  public void setwqps(double value){
    wqps=value;   
  }
  public double getwqps(){
   return wqps;  
  }
  public void setrqps(double value){
    rqps=value;   
  }
  
  public double getrqps(){
   return rqps;   
  }
  public void setdate(String value){
    date=value;   
  }
  
  public String getdate(){
   return date;   
  }
}
