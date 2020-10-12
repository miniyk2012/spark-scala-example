package com.javaexamples.sharedVariables.accumulators

import java.math.BigInteger

import com.holdenkarau.spark.testing.SharedSparkContext
import com.javaexamples.sharedVariables.accumulators.CustomAccumulatorDemo.BigIntegerAccumulator
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

  test("test customer accumulator") {
    val bigAcc = new BigIntegerAccumulator()
    val list = Seq(1,2,3,4,5)
    sc.register(bigAcc)
    sc.parallelize(list).map(r=>{
      bigAcc.add(new BigInteger("1"))
      r
    }).count()
    println(s"bigAcc=${bigAcc.value()}")
    assert(bigAcc.value.intValue === list.size)
  }
}
