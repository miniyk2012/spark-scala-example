package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MapTransformationMy {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )

    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),structureSchema)
    df2.printSchema()
    df2.show(false)
    import spark.implicits._

    val util = new Util()
    val df3 = df2.map(row=>{
      // 这里需要把util序列化到各个executor中, 因此比较费cpu和io
      val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
      (fullName, row.getString(3), row.getInt(5))
    })
    val df3Map = df3.toDF("fullName", "id", "salary")
    df3Map.printSchema()
    df3Map.show(false)

    val df4 = df2.mapPartitions(iter=> {
      val util = new Util()
      val res = iter.map(row=>{
        val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
        (fullName, row.getString(3), row.getInt(5))
      })
      res
    })
    val df4Part = df3.toDF("fullName", "id", "salary")
    df4Part.printSchema()
    df4Part.show(false)
  }
}
