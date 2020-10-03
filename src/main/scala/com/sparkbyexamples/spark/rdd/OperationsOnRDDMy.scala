package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object OperationsOnRDDMy extends App {
  val spark = SparkSession.builder()
    .appName("OperationsOnRDDMy")
    .master("local[1]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val rdd = spark.sparkContext.parallelize(
    List("Germany India USA","USA London Russia","Mexico Brazil Canada China")
  )
  val listRdd = spark.sparkContext.parallelize(List(9,2,3,4,5,6,7,8))

  val wordRdd = rdd.flatMap(_.split(" "))
  //sortBy
  println("Sort by word name")
  val sortRdd = wordRdd.sortBy(f=>f)
  sortRdd.foreach(println)
  println
  val groupRdd = wordRdd.groupBy(f=>f.length)
  groupRdd.sortBy(f=>f._1).foreach(println)
}
