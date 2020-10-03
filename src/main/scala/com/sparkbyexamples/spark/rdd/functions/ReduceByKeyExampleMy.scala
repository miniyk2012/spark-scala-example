package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object ReduceByKeyExampleMy extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  val rdd=spark.sparkContext.parallelize(data)
  val rdd1 = rdd.flatMap(v=>v.split(" ")).map((_,1)).reduceByKey(_+_)
  rdd1.foreach(println)
}
