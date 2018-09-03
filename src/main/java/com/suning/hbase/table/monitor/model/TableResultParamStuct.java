package com.suning.hbase.table.monitor.model;

import java.util.ArrayList;
import java.util.List;

public class TableResultParamStuct {
 private static  List<TableQps> qpslist = new ArrayList<TableQps>();
 private List<TablefsizeMsize> fmlist = new ArrayList<TablefsizeMsize>();
 private  List<TableRegionNum> rnlist = new ArrayList<TableRegionNum>();
 
 public  List<TableQps>  getTableQpsList(){
   
   return qpslist;
 }
 

 
 public  void  setTableQpsList(List<TableQps> list){
   
   qpslist= list;
 }
 
 public  void  setTablefsizeMsizeList(List<TablefsizeMsize> list){
   
   fmlist= list;
 }
 public  List<TablefsizeMsize>  getTablefsizeMsizeList(){
   
   return fmlist;
 }
 
 public  void  setTableRegionNumList(List<TableRegionNum> list){
   
   rnlist= list;
 }
 public  List<TableRegionNum>   getTableRegionNumList(){
   
   return rnlist;
 }

}
