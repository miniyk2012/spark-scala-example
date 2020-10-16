package com.sparkbyexamples.spark.dataframe.join

import com.javaexamples.util.SparkContextProvider

object JoinMultipleColumnsMy {
  def main(args: Array[String]): Unit = {
    SparkContextProvider.appName("JoinMultipleColumnsMy")
    val spark = SparkContextProvider.getSparkSession

    val emp = Seq((1,"Smith",-1,"2018",10,"M",3000),
      (2,"Rose",1,"2010",20,"M",4000),
      (3,"Williams",1,"2010",10,"M",1000),
      (4,"Jones",2,"2005",10,"F",2000),
      (5,"Brown",2,"2010",30,"",-1),
      (6,"Brown",2,"2010",50,"",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","branch_id","dept_id","gender","salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(("Finance", 10, "2018"),
      ("Marketing", 20, "2010"),
      ("Marketing", 20, "2018"),
      ("Sales", 30, "2005"),
      ("Sales", 30, "2010"),
      ("IT", 50, "2010")
    )

    val deptColumns = Seq("dept_name", "dept_id", "branch_id")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)

    //Using multiple columns on join expression
    empDF.join(deptDF, empDF("branch_id") === deptDF("branch_id")
      && empDF("dept_id") === deptDF("dept_id")
    ).show(false)

    //Using Join with multiple columns on filter clause
    empDF.join(deptDF).filter(empDF("dept_id") === deptDF("dept_id") &&
      empDF("branch_id") === deptDF("branch_id"))
      .show(false)

    //Using Join with multiple columns on where clause
    empDF.join(deptDF).where(empDF("dept_id") === deptDF("dept_id") &&
      empDF("branch_id") === deptDF("branch_id"))
      .show(false)

    //Using SQL & multiple columns on join expression
    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")

    val resultDF = spark.sql("select e.* from EMP e, DEPT d " +
      "where e.dept_id == d.dept_id and e.branch_id == d.branch_id")
    resultDF.show(false)
  }

}
