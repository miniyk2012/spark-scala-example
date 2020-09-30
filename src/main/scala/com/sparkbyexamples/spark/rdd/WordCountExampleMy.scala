package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object WordCountExampleMy extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val rdd = sc.textFile("src/main/resources/test.txt")
  println("initial partition count:" + rdd.getNumPartitions)

  val reparRdd = rdd.repartition(4)
  println("re-partition count:" + reparRdd.getNumPartitions)

  val rdd2 = reparRdd.flatMap(f => f.split(" "))
  val rdd3 = rdd2.map(m => (m, 1))
  val rdd4 = rdd3.filter(_._1.startsWith("a"))
    .reduceByKey(_ + _).map(a => (a._2, a._1)).sortByKey()
  rdd4.foreach(println)

  println("rdd4 count:" + rdd4.count())

  println("rdd4 first: " + rdd4.first())

  println("rdd4 max: " + rdd4.max())

  println("reduce: " + rdd4.reduce((a, b) => (a._1 + b._1, a._2)))

  val data3 = rdd4.take(3)
  for (x <- data3) {
    println(x)
  }

  val data4 = rdd4.collect()
  for (x <- data4) {
    println(x)
  }

  rdd4.saveAsTextFile("src/main/resources/store/wordCount")
}
