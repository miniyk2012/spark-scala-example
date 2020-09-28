package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDDMy extends App with BaseResource {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val rdd = spark.sparkContext.emptyRDD
  val rddString = spark.sparkContext.emptyRDD[String]
  println(rdd)
  println(rddString)
  println("Num of Partitions: " + rdd.getNumPartitions)
  println(resourceDirectory)
  rddString.saveAsTextFile(resourceDirectory + "store/emptyRddString")

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions: " + rdd2.getNumPartitions)

  // Pair RDD


}
