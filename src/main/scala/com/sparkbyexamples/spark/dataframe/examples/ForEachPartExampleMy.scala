package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object ForEachPartExampleMy {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"))

    val df = spark.createDataFrame(data).repartition(3).toDF("Product", "Amount", "Country")
    val i = spark.sparkContext.longAccumulator("i")
    df.foreachPartition(partitionOfRecords => {
      i.add(1)
      partitionOfRecords.foreach(println)
    })
    println(s"i=${i.value}, ${i}")

    println
    //rdd
    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9), 5)
    rdd.mapPartitions(r => r).foreach(println)

    println
    val j = spark.sparkContext.longAccumulator("j")
    rdd.mapPartitions(records => {
      j.add(1)
      records
    }).foreach(println)
    println(s"i=${j.value}, ${j}")  // 5个partition, j只能在driver端读取

    println
    var n = 0
    val newrdd = rdd.map(r => {
      n += 1
      r
    })
    newrdd.count()
    println("n=" + n)  // n=0, 因为driver中的变量没办法被executor中改变

    println
    val newrdd2 = rdd.map(r => {
      j.add(1)
      r
    })
    newrdd2.count()
    println(s"i=${j.value}, ${j}")  // 可以重复累加
  }

}
