package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object reduceExampleMy extends App {
  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))
  println("Total is :" + listRdd.reduce((acc, v) => acc + v))
  println("Min is :" + listRdd.reduce((acc, v) => acc min v))
  println("Max is :" + listRdd.reduce((acc, v) => acc max v))

  println()
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30),
    ("C", 40), ("B", 30), ("B", 60)))
  println("Total is :" + inputRDD.reduce((acc, v) => {
    ("Total", acc._2 + v._2)
  })._2)
  println("Min is :" + inputRDD.reduce((acc, v) => {
    ("Min", acc._2 min v._2)
  })._2)
  println("Max is :" + inputRDD.reduce((acc, v) => {
    ("Max", acc._2 max v._2)
  })._2)
}
