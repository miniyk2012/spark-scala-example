package com.sparkbyexamples.spark.rdd

trait BaseResource {
  val resourceDirectory = new java.io.File(".").getCanonicalPath + "/src/main/resources/"
}
