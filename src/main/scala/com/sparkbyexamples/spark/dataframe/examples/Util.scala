package com.sparkbyexamples.spark.dataframe.examples

// 最好继承Serializable, 否则如果有是从driver传输到excutor的对象, 则会报
// Caused by: java.io.NotSerializableException: com.sparkbyexamples.spark.dataframe.examples.Util
class Util extends Serializable {
  def combine(fname:String,mname:String,lname:String):String = {
    fname+","+mname+","+lname
  }
}
