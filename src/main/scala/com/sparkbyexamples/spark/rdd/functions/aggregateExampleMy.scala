package com.sparkbyexamples.spark.rdd.functions
import org.apache.spark.sql.SparkSession

object aggregateExampleMy  extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2), 10)
  println("output 1 : " + listRdd.aggregate(0)(_ + _, _ + _))

  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)))
  println("output 2 : " + inputRDD.map(v => v._2).reduce(_+_))
  println("Number of Partitions :" + listRdd.getNumPartitions)
  //aggregate example
  def param1 = (acc1:Int, acc2:Int) => acc1 + acc2
  def param2 = (acc1:Int, acc2:Int) => acc1 + acc2
  println("output 3 : "+listRdd.aggregate(1)(param1, param2))

}
