package com.suning.hbase.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyHelper {
  private static String configPropertiesPath = "conf\\properties";
  public static Properties props = new Properties();
  private static Logger LOG = LoggerFactory.getLogger(PropertyHelper.class);
          
   
    /**
    * read value from file 
    * @param key
    *            
    * @return String
    */
    public static String getKeyValue(String key) {
        return props.getProperty(key);
    }
    /**
    * read value
    * @param filePath 
    * @param key 
    */ 
    public static String readValue(String filePath, String key) {
        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    filePath));
            props.load(in);
            String value = props.getProperty(key);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
    * update or write properties
    * if exist update it
    * !exist  the write it
    */ 
    public static void writeProperties(String keyname,String keyvalue) {  

        try {
            OutputStream fos = new FileOutputStream(configPropertiesPath);
            props.setProperty(keyname, keyvalue);
            props.store(fos, "Update '" + keyname + "' value");
        } catch (IOException e) {
            System.err.println("update failed");
        }
    }
    /**
    * update properties
    * 
    */ 
    public static void updateProperties(String keyname,String keyvalue) {
        try {
            props.load(new FileInputStream(configPropertiesPath));
            OutputStream fos = new FileOutputStream(configPropertiesPath);            
            props.setProperty(keyname, keyvalue);
            props.store(fos, "Update '" + keyname + "' value");
        } catch (IOException e) {
            System.err.println("update failed");
        }
    }
    
    public static boolean setPropertiesPath(String filePath){
    	File file=new File(filePath); 
    	
      
      if(!file.exists()){
        LOG.error("the file not exists:"+filePath);
        return false;
      }
         

      try {
    	InputStream in=new FileInputStream(filePath);
        PropertyHelper.props.load(in);
      } catch (IOException e) {
        
        e.printStackTrace();
      }
      return true;
    
    }
    
    public static String getPropertiesPath(){
   
      return configPropertiesPath;
      
    }

  public static void main(String[] args) {
		String rownum = null;
		PropertyHelper.setPropertiesPath("conf//config.properties");
    System.out.println(PropertyHelper.getKeyValue("zookeeper.znode.parent"));
  }
}