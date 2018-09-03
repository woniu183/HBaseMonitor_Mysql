package com.suning.hbase.table.monitor.service;

import org.apache.log4j.PropertyConfigurator;

import com.suning.hbase.util.PropertyHelper;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledCaptureHBaseTableTaram extends CaptureHBaseTableParam {
    static {
        PropertyHelper.setPropertiesPath("conf/config.properties");
        PropertyConfigurator.configureAndWatch("conf/log4j.properties", 5000);
    }

    public static void main(String[] args) {

        CaptureHBaseClusterParamBaseOnRegion  CaptureHBaseClusterParamBaseOnRegion = new CaptureHBaseClusterParamBaseOnRegion();
        // CaptureHBaseTableParam   capturehbasetableTaram  = new CaptureHBaseTableParam();
        //ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService service1 = Executors.newSingleThreadScheduledExecutor();
        //service.scheduleAtFixedRate(capturehbasetableTaram, 0, 5, TimeUnit.MINUTES);
        //Thread capturehbasetableTaramthread = new Thread(CaptureHBaseClusterParamBaseOnRegion);
        //capturehbasetableTaramthread.start();
        service1.scheduleAtFixedRate(CaptureHBaseClusterParamBaseOnRegion, 0, 1, TimeUnit.MINUTES);
    }

}
