package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner


object PartitionByMy {
  def main(args:Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile("src/main/resources/zipcodes.csv")
    val rdd2:RDD[Array[String]] = rdd.map(m=>m.split(","))
    rdd2.foreach(line=>{
      for (e <- line) {
        print(e + " ")
      }
      println
    })
    println
    val rdd3 = rdd2.map(a => (a(1), a.mkString(",")))
    rdd3.foreach(println)

    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))
    rdd4.saveAsTextFile("src/main/resources/store/partitionMy")
  }
}
