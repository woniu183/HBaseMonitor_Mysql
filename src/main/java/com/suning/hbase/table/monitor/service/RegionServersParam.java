package com.suning.hbase.table.monitor.service;

import java.util.HashMap;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.suning.hbase.util.HBaseUtil;

public class RegionServersParam {
  public   HashMap<String,HashMap<String,String>>  getRegionServersParam(HBaseAdmin admin){  
    if(admin.isAborted()){  
      admin=  HBaseUtil.getAdmin();   
    }
    return HBaseUtil.getRegionServerParam( admin);   
  }

}
