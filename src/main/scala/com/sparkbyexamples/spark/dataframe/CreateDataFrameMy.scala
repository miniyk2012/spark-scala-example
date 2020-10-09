package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreateDataFrameMy {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("CreateDataFrameMy")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    import spark.implicits._
    //From RDD (USING toDF())
    val dfFromRDD1 = rdd.toDF(columns: _*)
    dfFromRDD1.printSchema()

    println
    //From RDD (USING createDataFrame)
    // : _* unpack 解包
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns: _*)
    dfFromRDD2.printSchema()

    println
    //From RDD (USING createDataFrame and Adding schema using StructType)
    //convert RDD[T] to RDD[Row]
    val schema = StructType(columns.map(fieldName =>
      StructField(fieldName, StringType, true)))
    val rowRDD = rdd.map(att => Row(att._1, att._2))
    val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)
    dfFromRDD3.printSchema()

    println
    //From Data (USING createDataFrame and Adding schema using StructType)
    import scala.collection.JavaConversions._
    val rowData = data
      .map(attributes => Row(attributes._1, attributes._2))
    var dfFromData3 = spark.createDataFrame(rowData, schema)
    dfFromData3.printSchema()
  }
}
