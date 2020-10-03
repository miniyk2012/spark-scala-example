package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object OperationsOnRDDMy extends App {
  val spark = SparkSession.builder()
    .appName("OperationsOnRDDMy")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val rdd = spark.sparkContext.parallelize(
    List("Germany India USA","USA London Russia","Mexico Brazil Canada China")
  )
  val listRdd = spark.sparkContext.parallelize(List(9,2,3,4,5,6,7,8))

}
