package com.javaexamples.sharedVariables.accumulators;

import com.javaexamples.util.SparkContextProvider;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;


public class AccumulatorSubtraction {
    public static class SubtractionAccumulator extends AccumulatorV2<Integer, Integer> {
        private Integer num = 0;

        public SubtractionAccumulator() {
        }

        public SubtractionAccumulator(Integer num) {
            this.num = num;
        }

        @Override
        public boolean isZero() {
            return num.equals(0);
        }


        @Override
        public AccumulatorV2<Integer, Integer> copy() {
            return new SubtractionAccumulator(num);
        }

        @Override
        public void reset() {
            num = 0;
        }

        @Override
        public void add(Integer v) {

        }


        public void sub(Integer v) {
            num = -v;
        }

        @Override
        public void merge(AccumulatorV2<Integer, Integer> other) {
            num += other.value();
        }

        @Override
        public Integer value() {
            return num;
        }
    }

    public static void main(String[] args) {
        SparkContextProvider.appName("AccumulatorSubtraction");
        SparkContext sc = SparkContextProvider.getSparkContext();
        SparkSession spark = SparkContextProvider.getSparkSession();
        SubtractionAccumulator sa = new SubtractionAccumulator();
        sc.register(sa, "SubtractionAccumulator");
        Dataset<Integer> num1 = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
        num1.foreach(x -> {
            sa.sub(1);
        });
        // 因为是Action操作，会被立即执行所以打印的结果是符合预期的
        System.out.println("num: " + sa.value()); // -6
    }

}
