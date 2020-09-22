package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object RDDRepartitionExampleMy extends App with BaseResource {
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  val rdd = spark.sparkContext.parallelize(Range(0, 20))
  println("From local[5]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
  rdd1.saveAsTextFile(resourceDirectory + "store/rdd_partition_example")
  println("parallelize : "+rdd1.partitions.size)

  val rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
  println("TextFile : "+rddFromFile.partitions.size)

  val rdd2 = rdd1.repartition(4)
  println("Repartition size : "+rdd2.partitions.size)
  rdd2.saveAsTextFile("src/main/resources/store/re-partition")

  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : "+rdd3.partitions.size)
  rdd3.saveAsTextFile("src/main/resources/store/coalesce")

}
