package com.suning.hbase.table.monitor.model;

public class ClusterTotalRequest {
  private   String starttime ;
  private   long clusterrequestcount ;
  private   long clusterreadrequestcount;
  private   long clusterwriterequestcount;
  private   long clusterregionnum ;
  private   long clustermaxheapMb ;
  private   long clusteruseheapMb ;
  

  public String getStartTime(){
    return starttime;
  }
  public void setClusterregionnum(long value){
    clusterregionnum = value;
  }
  public  long getClusterregionnum(){
    return clusterregionnum;
  }
  
  public void setClustermaxheapM(long value){
    clustermaxheapMb = value;
  }
  public  long getClustermaxheapM(){
    return clustermaxheapMb;
  }
  
  public void setClusteruseheapMb(long value){
    clusteruseheapMb = value;
  }
  public  long getClusteruseheapMb(){
    return clusteruseheapMb;
  }
  
  public void setClusterRequestCount(long value){
    clusterrequestcount = value;
  }
  public  long getClusterRequestCount(){
    return clusterrequestcount;
  }
  
  public void setClusterReadRequestCount(long value){
    clusterreadrequestcount = value;
  }
  public  long getClusterReadRequestCount(){
    return clusterreadrequestcount;
  }
  public void setClusterWriteRequestCount(long value){
    clusterwriterequestcount = value;
  }
  public  long getClusterWriteRequestCount(){
    return clusterwriterequestcount;
  }

  @Override
  public String toString() {
    return "ClusterTotalRequest{" +
            "starttime='" + starttime + '\'' +
            ", clusterrequestcount=" + clusterrequestcount +
            ", clusterreadrequestcount=" + clusterreadrequestcount +
            ", clusterwriterequestcount=" + clusterwriterequestcount +
            ", clusterregionnum=" + clusterregionnum +
            ", clustermaxheapMb=" + clustermaxheapMb +
            ", clusteruseheapMb=" + clusteruseheapMb +
            '}';
  }

  public void setStartTime(String value) {
    starttime= value;
    
  }
  
  
  
}
