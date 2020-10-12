package com.javaexamples.sharedVariables.accumulators;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;

import java.math.BigInteger;

public class CustomAccumulatorDemo {
    public static class BigIntegerAccumulator extends AccumulatorV2<BigInteger, BigInteger> {
        private BigInteger num = BigInteger.ZERO;

        public BigIntegerAccumulator(BigInteger num) {
            this.num = new BigInteger(num.toString());
        }

        @Override
        public boolean isZero() {
            return num.compareTo(BigInteger.ZERO) == 0;
        }

        @Override
        public AccumulatorV2<BigInteger, BigInteger> copy() {
            return new BigIntegerAccumulator(num);
        }

        @Override
        public void reset() {
            num = BigInteger.ZERO;
        }

        @Override
        public void add(BigInteger v) {
            this.num = this.num.add(num);
        }

        @Override
        public void merge(AccumulatorV2<BigInteger, BigInteger> other) {
            num = num.add(other.value());
        }

        @Override
        public BigInteger value() {
            return num;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        SparkContext sc = spark.sparkContext();


    }
}
