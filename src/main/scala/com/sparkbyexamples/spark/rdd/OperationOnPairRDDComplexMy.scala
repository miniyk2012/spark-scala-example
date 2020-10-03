package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object OperationOnPairRDDComplexMy extends App {
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  //Create key value pairs
  val data = spark.sparkContext.parallelize(keysWithValuesList)
  val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
  val init = new mutable.HashSet[String]

  def param1 = (acc: mutable.HashSet[String], v: String) => acc += v

  def param2 = (acc1: mutable.HashSet[String], acc2: mutable.HashSet[String]) => acc1 ++= acc2

  kv.aggregateByKey(init)(param1, param2).foreach(println)
  //    Aggregate By Key unique Results
  //    bar -> C,D
  //    foo -> B,A

  val param3 = (acc: Int, v: String) => acc + 1
  val param4 = (acc1: Int, acc2: Int) => acc1 + acc2
  kv.aggregateByKey(0)(param3, param4).foreach(println)
  //    Aggregate By Key sum Results
  //    bar -> 3
  //    foo -> 5
  println()
  //ReducebyKey returns same datatype
  val studentRDD = spark.sparkContext.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)
    .cache()
  val studentKey = studentRDD.map(e => (e._1, (e._2, e._3)))
  studentKey.reduceByKey((accu, v) => {
    if (accu._2 > v._2) accu else v
  }).foreach(println)

  println()
  //Similar example with aggregateByKey
  val def1 = (acc: (String, Int), v: (String, Int)) => if (acc._2 > v._2) acc else v
  studentKey.aggregateByKey(("", 0))(def1, def1).foreach(println)

  println()
  val subject = studentRDD.map(v => (v._2, (v._1, v._3)))
  subject.aggregateByKey(("", 0))(def1, def1).foreach(println)
  // 每个学科的最高分
  //    (Chemistry,(Jimmy,97))
  //    (Biology,(Tina,87))
  //    (Maths,(Thomas,87))
  //    (Physics,(Thomas,93))

  println()
  // 求每个学生的总分
  val studentTotals = studentKey.map(v => (v._1, v._2._2)).reduceByKey(_ + _).cache()
  studentTotals.foreach(println)

  //Student with highest score
  println()
  val tot = studentTotals.max()(new Ordering[Tuple2[String, Int]]() {
    override def compare(x: (String, Int), y: (String, Int)): Int =
      Ordering[Int].compare(x._2, y._2)
  })
  println("First class student : " + tot._1 + "=" + tot._2)
}
