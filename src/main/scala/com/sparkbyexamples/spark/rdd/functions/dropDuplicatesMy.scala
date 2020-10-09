package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object dropDuplicatesMy extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("dropDuplicatesMy")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.sqlContext.implicits._
  val columns = Seq("language", "searchid", "ideaid")
  val data = Seq(("Java", "20000", "123"), ("Python", "20000", "123"), ("Scala", "3000", "234"))
  val rdd = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd.toDF(columns: _*)
  dfFromRDD1.printSchema()
  dfFromRDD1.show()

  println
  val df2 = dfFromRDD1.dropDuplicates(Seq("searchid", "ideaid"))
  df2.show()
}
