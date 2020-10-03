package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object OperationsOnPairRDDMy extends App {
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local[5]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val rdd = spark.sparkContext.parallelize(
    List("Germany India USA", "USA India Russia", "India Brazil Canada China")
  )
  println(rdd.getNumPartitions)
  val wordsRdd = rdd.flatMap(_.split(" "))
  val pairRDD = wordsRdd.map(f => (f, 1))
  pairRDD.foreach(println)

  println()
  pairRDD.distinct().foreach(println)

  println()
  pairRDD.sortByKey().foreach(println)

  println()
  println("Reduce by Key ==>")
  pairRDD.reduceByKey(_ + _).foreach(println)
  println(pairRDD.getNumPartitions)
  println()
  println("aggregate by Key ==>")

  def param1 = (accu: Int, v: Int) => accu + v

  def param2 = (accu1: Int, accu2: Int) => accu1 + accu2

  val wordCount2 = pairRDD.aggregateByKey(0)(param1, param2)
  wordCount2.foreach(println)

  println()
  println("Keys ==>")
  println(pairRDD.keys.foreach(println(_)))
  println("\nKeys ==>")
  for (key <- pairRDD.keys.collect()) {
    println(key)
  }

  println()
  println("Count :" + wordCount2.count())

  println("collectAsMap ==>")
  // collectAsMap将key-value型的RDD转换为Scala的map
  // map中如果有相同的key，其value只保存最后一个值。
  pairRDD.collectAsMap().foreach(println)
}
