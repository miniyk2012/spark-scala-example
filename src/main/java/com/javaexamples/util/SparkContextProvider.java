package com.javaexamples.util;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextProvider {
    private static SparkSession spark;
    private static SparkContext sc;
    private static String appName="AppName";

    private SparkContextProvider() {
        initSpark();
    }
    private static class StaticSingletonHolder {
        private static final SparkContextProvider instance = new SparkContextProvider();
    }

    public static void appName(String appName) {
        SparkContextProvider.appName = appName;
    }
    private static void initSpark() {
        spark = SparkSession.builder().appName(appName).master("local[*]")
                .config("spark.eventLog.dir", "file:///Users/admin/data/spark/spark-logs")
                .config("spark.eventLog.enabled", "true")
                .getOrCreate();
        sc = spark.sparkContext();
        sc.setLogLevel("WARN");
    }

    public static SparkContext getSparkContext() {
        return StaticSingletonHolder.instance.sc;
    }

    public static SparkSession getSparkSession() {
        return StaticSingletonHolder.instance.spark;
    }
}
