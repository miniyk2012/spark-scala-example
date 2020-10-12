package com.javaexamples.sharedVariables.accumulators

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

class CustomAccumulatorDemoTest extends FunSuite
  with SharedSparkContext
  with Matchers {
  override implicit def reuseContextIfPossible: Boolean = true
  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    rdd.count should equal(list.length)
    assert(rdd.count === list.size)
  }
}
