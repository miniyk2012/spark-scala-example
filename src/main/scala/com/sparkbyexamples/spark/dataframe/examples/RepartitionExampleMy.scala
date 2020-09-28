package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

object RepartitionExampleMy extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    //    .config("spark.default.parallelism", "500")
    .getOrCreate()

  // spark.sqlContext.setConf("spark.default.parallelism", "500")
  //spark.conf.set("spark.default.parallelism", "500")
  val df = spark.range(0,20)
  df.printSchema()
  println("df", df.rdd.partitions.length)

  df.write.mode(SaveMode.Overwrite).csv("src/main/resources/store/DFRepartitionMydf1")

  val df2 = df.repartition(3)
  println("df2", df2.rdd.partitions.length)
  df2.write.mode(SaveMode.Overwrite).csv("src/main/resources/store/DFRepartitionMydf2")

  val df3 = df.coalesce(2)
  println("df3", df3.rdd.partitions.length)
  df3.write.mode(SaveMode.Overwrite).csv("src/main/resources/store/DFRepartitionMydf3")

  val df4 = df.groupBy("id").count()
  df4.printSchema()
  df4.show()
  println("df4", df4.rdd.getNumPartitions)
}
