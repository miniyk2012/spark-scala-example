package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayToColumn extends App {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val arrayData = Seq(
    Row("James",List("Java","Scala","C++")),
    Row("Michael",List("Spark","Java","C++")),
    Row("Robert",List("CSharp","VB",""))
  )

  val arraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(StringType))

  val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
  arrayDF.printSchema()
  arrayDF.show()

//  val arrayDFColumn = df.select(
//    df("name") +: (0 until 2).map(i => df("subjects")(i).alias(s"LanguagesKnown$i")): _*
//  )
//
//  arrayDFColumn.show(false)

  //How to convert Array of Array to column
  val arrayArrayData = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )

  val arrayArraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(ArrayType(StringType)))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  df.printSchema()
  df.show()

  val df2 = df.select(
    (df("name") +: (0 until 2).map(i => df("subjects")(i).alias(s"LanguagesKnown$i"))): _*
  )

  df2.show(false)

  println
  // 也可以直接用sql的表来做, 功能是一样的
  df.createOrReplaceTempView("table1")
  val df3 = spark.sql(
    """
      |select name, subjects[0] LanguagesKnown0, subjects[1] LanguagesKnown1 from table1
      |""".stripMargin)
  df3.show(truncate = false)

}
