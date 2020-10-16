package com.sparkbyexamples.spark.dataframe

import com.javaexamples.util.SparkContextProvider
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WithColumnMy {
  def main(args: Array[String]): Unit = {
    SparkContextProvider.appName("WithColumnMy")
    val spark = SparkContextProvider.getSparkSession

    val arrayStructureData = Seq(
      Row(Row("James ","","Smith"),"1","M",3100,List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
      Row(Row("Michael ","Rose",""),"2","M",3100,List("Tennis"),Map("hair"->"brown","eye"->"black")),
      Row(Row("Robert ","","Williams"),"3","M",3100,List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
      Row(Row("Maria ","Anne","Jones"),"4","M",3100,null,Map("hair"->"blond","eye"->"red")),
      Row(Row("Jen","Mary","Brown"),"5","M",3100,List("Blogging"),Map("white"->"black","eye"->"black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("id",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
      .add("Hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
    println("df2")
    df2.printSchema()
    df2.show(false)

    //Change the column data type
    val df3 = df2.withColumn("salary", df2("salary").cast("Integer"))
    df3.printSchema()

    //Derive a new column from existing
    val df4 = df2.withColumn("CopiedColumn", df2("salary") * -1)
    df4.printSchema()
    val df5 = df4.withColumn("CopiedColumn", col("CopiedColumn").cast("Integer"))
    println("df5")
    df5.printSchema()
    df5.show(false)

    //Transforming existing column
    println("df6")
    val df6 = df2.withColumn("salary", df2("salary") * 100)
    df6.printSchema()

    //You can also chain withColumn to change multiple columns
    //Renaming a column.
    val df7 = df2.withColumnRenamed("gender", "sex")
    df7.printSchema()

    // 也能增加一列
    val df8 = df2.withColumn("sex", col("gender"))
    df8.printSchema()

    //Droping a column
    val df9=df2.drop("sex")
    df9.printSchema()

    //Adding a literal value
    val df10 = df2.withColumn("Country", lit("USA"))
    df10.printSchema()
    df10.show(false)

    //Retrieving
    df2.show(false)
    df2.select("name").show(false)
    df2.select("name.firstname").show(false)
    df2.select("name.*").show(false)

    // sql select
    println("sql select")
    df2.createOrReplaceTempView("PERSON")
    spark.sql("select name.* from PERSON").show(false)

    df2.select(col("hobbies")).show()
    val df11 = df2.select(col("*"),explode(col("hobbies")))
    df11.show(false)

    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"), ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    val dfFromData = spark.createDataFrame(data).toDF(columns:_*)
    dfFromData.printSchema()
    dfFromData.show(false)

    import spark.implicits._
    val newDF = dfFromData.map(f=>{
      val nameSplit = f.getAs[String](0).split(",")
      val addSplit = f.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3).trim())
    })
    println(newDF.first())
    val finalDF = newDF.toDF("First Name","Last Name","Address Line1","City","State","zipCode")
    println("finalDF")
    finalDF.printSchema()
    finalDF.show(false)
    val df12 =  finalDF.withColumn("zipCode", finalDF("zipCode").cast("Integer"))
    df12.printSchema()
    df12.show(false)

    finalDF.createOrReplaceTempView("PERSON2")
    val df13 = spark.sql("select zipCode, cast(zipCode as int) zipCode2 from PERSON2")
    df13.printSchema()
    df13.show(false)
  }
}
