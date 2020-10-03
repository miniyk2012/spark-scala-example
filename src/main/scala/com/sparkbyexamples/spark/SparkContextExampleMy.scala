package com.sparkbyexamples.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext

object SparkContextExampleMy extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkContextExampleMy")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sparkContext:SparkContext=spark.sparkContext
  val sqlCon:SQLContext=spark.sqlContext

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName)
  println("Deploy Mode :"+spark.sparkContext.deployMode)
  println("Master :"+spark.sparkContext.master)

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();
  println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);
}
