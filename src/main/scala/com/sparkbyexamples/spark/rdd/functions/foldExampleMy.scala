package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object foldExampleMy extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("foldExampleMy")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //fold example
  val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))
  println("Partitions : " + listRdd.getNumPartitions)
  println("Total is: " + listRdd.fold(0)(_ + _))
  println("Total with init value 2 : " + listRdd.fold(2)((acc, ele) => {
    acc + ele
  }))
  println("Min : " + listRdd.fold(Int.MaxValue)((acc, v) => {
    acc min v
  }))
  println("Max : " + listRdd.fold(Int.MinValue)((acc, v) => {
    acc max v
  }))
  println()
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)))
  println("Total : "+inputRDD.fold("", 0)((acc,v)=>{("Total", acc._2+v._2)}))
  println("Min : " + inputRDD.fold("", Int.MaxValue)((acc,v)=>{("Min", acc._2 min v._2)}))
  println("Max : " + inputRDD.fold("", Int.MinValue)((acc,v)=>{("Max", acc._2 max v._2)}))
}
