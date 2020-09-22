package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object RepartitionExampleMy extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    //    .config("spark.default.parallelism", "500")
    .getOrCreate()

  // spark.sqlContext.setConf("spark.default.parallelism", "500")
  //spark.conf.set("spark.default.parallelism", "500")
  val df = spark.range(0,20)
  val df4 = df.groupBy("id").count()
  println(df4.rdd.getNumPartitions)
}
