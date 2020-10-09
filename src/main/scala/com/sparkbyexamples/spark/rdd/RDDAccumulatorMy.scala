package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object RDDAccumulatorMy extends App{
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))
  rdd.foreach(longAcc.add(_))
  println(longAcc.value)
}
