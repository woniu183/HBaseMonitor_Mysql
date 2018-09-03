package com.suning.hbase.table.monitor.model;

import java.util.HashMap;

public class RsTablesParamStruct {
      private   HashMap<String, HashMap<String, String>> tablesParamMap ;
      private   HashMap<String, HashMap<String, String>> regionserverParamMap;
      
      public void  setTablesParaList(HashMap<String, HashMap<String, String>> list){
        tablesParamMap=list;
      }
      public HashMap<String, HashMap<String, String>> getTablesParaList(){
        return tablesParamMap;
        
      }
      public void setRegionserverParaList(HashMap<String, HashMap<String, String>> list){
        regionserverParamMap = list;      
      }
      public  HashMap<String, HashMap<String, String>> getRegionserverParaList(){
        return regionserverParamMap;
      }

}
