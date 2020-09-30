package com.sparkbyexamples.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AccDemoMy extends App {
  def error_acc(sc: SparkContext) = {
    val accum= sc.accumulator(0, "Error Accumulator")
    val data = sc.parallelize(1 to 10)
    //用accumulator统计偶数出现的次数，同时偶数返回0，奇数返回1

    val newData = data.map{x => {
      if(x%2 == 0){
        accum += 1
        0
      }else 1
    }}
    //使用action操作触发执行
    newData.count
    //此时accum的值为5，是我们要的结果
    println(accum.value)
    //继续操作，查看刚才变动的数据,foreach也是action操作
    newData.foreach(x=>x)

    //上个步骤没有进行累计器操作，可是累加器此时的结果已经是10了
    //这并不是我们想要的结果
    println(accum.value)
  }
  def correct_acc(sc: SparkContext) = {
    val accum= sc.accumulator(0, "Error Accumulator")
    val data = sc.parallelize(1 to 10)
    //用accumulator统计偶数出现的次数，同时偶数返回0，奇数返回1

    val newData = data.map{x => {
      if(x%2 == 0){
        accum += 1
        0
      }else 1
    }}
    //使用action操作触发执行
    newData.cache()
    newData.count
    //此时accum的值为5，是我们要的结果
    println(accum.value)
    //继续操作，查看刚才变动的数据,foreach也是action操作
    newData.foreach(x=>x)

    //上个步骤没有进行累计器操作，可是累加器此时的结果已经是10了
    //这并不是我们想要的结果
    println(accum.value)
  }
  def action_acc(sc: SparkContext): Unit = {
    val accum = sc.accumulator(0, "Example Accumulator")
    val data = sc.parallelize(1 to 10)
    data.foreach(x=> accum += 1)
    data.count()
    println(accum.value)
  }
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  error_acc(sc)
  println()
  correct_acc(sc)
  println()
  action_acc(sc)
  spark.stop()
}
