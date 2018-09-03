package com.suning.hbase.table.monitor.model;

public class RegionFileSize implements Comparable<RegionFileSize> {
  private String regionname;
  private long filesize;
  private long filenum;
  public String getRegionname() {
    return regionname;
  }
  public void setRegionname(String regionname) {
    this.regionname = regionname;
  }
  public long getFilesize() {
    return filesize;
  }
  public void setFilesize(int filesize) {
    this.filesize = filesize;
  }
  public long getFilenum() {
    return filenum;
  }
  public void setFilenum(int filenum) {
    this.filenum = filenum;
  }
  @Override
  public int compareTo(RegionFileSize arg0) {   
    return (int)(arg0.getFilesize()- this.getFilesize());
  }

}
