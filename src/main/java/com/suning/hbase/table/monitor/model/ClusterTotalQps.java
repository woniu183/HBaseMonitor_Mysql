package com.suning.hbase.table.monitor.model;

public class ClusterTotalQps {

  private String date; 
  private double   qps ;
  private double   wqps;
  private double   rqps;
  private   long clusterregionnum ;
  private   long clustermaxheapMb ;
  private   long clusteruseheapMb ;
  
  public void setClusteruseheapMb(long value){
    clusteruseheapMb = value;
  }
  public  long getClusteruseheapMb(){
    return clusteruseheapMb;
  }
  public void setClustermaxheapM(long value){
    clustermaxheapMb = value;
  }
  public  long getClustermaxheapM(){
    return clustermaxheapMb;
  }
  
  public void setClusterregionnum(long value){
    clusterregionnum = value;
  }
  public  long getClusterregionnum(){
    return clusterregionnum;
  }
  
  
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
