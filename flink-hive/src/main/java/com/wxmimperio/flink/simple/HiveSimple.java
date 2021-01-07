package com.wxmimperio.flink.simple;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HiveSimple.java
 * @description This is the description of HiveSimple.java
 * @createTime 2020-11-06 11:39:00
 */
public class HiveSimple {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir = "/Users/weiximing/code/github/flink-best-practice/flink-hive/src/main/resources/hive-conf"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

    }
}
