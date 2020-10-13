package com.javaexamples.sharedVariables.accumulators;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

public class CustomAccumulatorDemo {

    public static class BigIntegerAccumulator extends AccumulatorV2<BigInteger, BigInteger> {
        private BigInteger num = BigInteger.ZERO;

        public BigIntegerAccumulator() {
        }

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
            this.num = this.num.add(v);
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
        sc.setLogLevel("WARN");
        // 直接new自定义的累加器
        BigIntegerAccumulator bigIntegerAccumulator = new BigIntegerAccumulator();
        sc.register(bigIntegerAccumulator, "bigIntegerAccumulator");

        List<BigInteger> numList = Arrays.asList(new BigInteger("9999999999999999999999"), new BigInteger("9999999999999999999999"), new BigInteger("9999999999999999999999"));
        Dataset<BigInteger> num = spark.createDataset(numList, Encoders.kryo(BigInteger.class));
        Dataset<BigInteger> num2 = num.map((MapFunction<BigInteger, BigInteger>) x -> {
            bigIntegerAccumulator.add(x);
            return x;
        }, Encoders.kryo(BigInteger.class));

        num2.count();
        System.out.println("bigIntegerAccumulator1: " + bigIntegerAccumulator.value());
        num2.count();
        System.out.println("bigIntegerAccumulator2: " + bigIntegerAccumulator.value());
    }
}
