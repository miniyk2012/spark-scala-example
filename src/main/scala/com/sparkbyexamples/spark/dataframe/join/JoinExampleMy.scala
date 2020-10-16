package com.sparkbyexamples.spark.dataframe.join

import com.javaexamples.util.SparkContextProvider

object JoinExampleMy {
  def main(args: Array[String]): Unit = {
    SparkContextProvider.appName("JoinMultipleDataFramesMy")
    val spark = SparkContextProvider.getSparkSession

    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )

    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.show(false)

    println("Inner join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner").drop("emp_dept_id")
      .show(false)

    println("Outer join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"outer")
      .show(false)

    println("full join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"full")
      .show(false)

    println("fullouter join")  // full, outer, fullouter都一样
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter")
      .show(false)

    println("right join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"right")
      .show(false)
    println("rightouter join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"rightouter")
      .show(false)

    println("left join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"left")
      .show(false)
    println("leftouter join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftouter")
      .show(false)

    // leftanti join返回左边表的记录, 前提是其记录对于右边表不满足ON语句中的判定条件
    println("leftanti join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
      .show(false)

    // 左半开连接会返回左边表的记录，前提是其记录对于右边表满足ON语句中的判定条件. 这是一种优化计算
    println("leftsemi join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi")
      .show(false)

    println("cross join")  // 由于有condition, 因此就是inner join
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"cross")
      .show(false)

    println("Using crossJoin()")
    empDF.crossJoin(deptDF).show(false)

    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")
    //SQL JOIN
    val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    joinDF.show(false)

    val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    joinDF2.show(false)
  }


}
