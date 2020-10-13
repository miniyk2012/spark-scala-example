package com

import com.holdenkarau.spark.testing.Utils
import org.apache.spark.sql.SparkSession

trait LocalSparkConf {
  val localSpark = setUpLocalSpark()

  def setUpLocalSpark(): org.apache.spark.sql.SparkSession = {
    /**
     * Constructs a configuration for hive, where the metastore is located in a
     * temp directory.
     */
    def newBuilder() = {
      val localMetastorePath = "/Users/admin/Documents/hive/metastore_db"
      val localWarehousePath = "/Users/admin/Documents/hive/spark-warehouse"
      val builder = SparkSession.builder()
        .appName("localSpark")
          .master("local[*]")
      // Long story with lz4 issues in 2.3+
      builder.config("spark.io.compression.codec", "snappy")
      // We have to mask all properties in hive-site.xml that relates to metastore
      // data source as we used a local metastore here.
      import org.apache.hadoop.hive.conf.HiveConf
      builder.config(HiveConf.ConfVars.METASTOREURIS.varname, "")
      builder.config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      builder.config("datanucleus.rdbms.datastoreAdapterClassName",
        "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      builder.config("hive.exec.dynamic.partition.mode", "nonstrict")
      builder.config("hive.exec.dynamici.partition", true)
      builder.config("spark.sql.streaming.checkpointLocation",
        Utils.createTempDir().toPath().toString)
      builder.config("spark.sql.warehouse.dir",
        localWarehousePath)
      // Enable hive support if available
      try {
        builder.enableHiveSupport()
      } catch {
        // Exception is thrown in Spark if hive is not present
        case e: IllegalArgumentException =>
      }
      builder
    }

    val builder = newBuilder()

    builder.getOrCreate()
  }
}
