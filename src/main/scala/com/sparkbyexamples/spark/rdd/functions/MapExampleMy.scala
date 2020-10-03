package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object MapExampleMy extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkSession")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val data = Seq("Project",
    "Gutenberg’s",
    "Alice’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s")

  val rdd=spark.sparkContext.parallelize(data)
  val rdd2 = rdd.map((_, 1))
  rdd2.reduceByKey(_+_).foreach(println)
}
