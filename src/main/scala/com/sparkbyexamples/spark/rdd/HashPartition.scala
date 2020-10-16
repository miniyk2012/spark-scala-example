package com.sparkbyexamples.spark.rdd
import org.apache.spark.HashPartitioner
import com.javaexamples.util.SparkContextProvider

object HashPartition {
  def main(args: Array[String]): Unit = {
    SparkContextProvider.appName("HashPartition")
    val sc = SparkContextProvider.getSparkContext

    val part = sc.parallelize(1 to 100)
    println(part.partitioner.getOrElse("aaa"))

    val group_rdd1 = part.map(x=>(x,x))
      .groupByKey(10)
    println(group_rdd1.partitioner.getOrElse("aaa"))

    val group_rdd2 = part.map(x=>(x,x))
      .groupByKey(new HashPartitioner(4))
    println(group_rdd2.partitioner.getOrElse("aaa"))
    println(group_rdd2.partitions.size)
    for (p <- group_rdd2.partitions) {
      println(p)
    }
  }

}
