package com.javaexamples.sharedVariables.accumulators;

import com.javaexamples.util.SparkContextProvider;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class AccumulatorTrapDemo {

    public static void main(String[] args) {
        SparkContext sc = SparkContextProvider.getSparkContext();
        SparkSession spark = SparkContextProvider.getSparkSession();

        LongAccumulator longAccumulator = sc.longAccumulator("long-account");

        // ------------------------------- 在transform算子中的错误使用 -------------------------------------------
        Dataset<Integer> num1 = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
        Dataset<Integer> nums2 = num1.map((MapFunction<Integer, Integer>) x -> {
            longAccumulator.add(1);
            return x;
        }, Encoders.INT());

        // 因为没有Action操作，nums.map并没有被执行，因此此时广播变量的值还是0
        System.out.println("num2 1: " + longAccumulator.value()); // 0

        // 调用一次action操作，num.map得到执行，广播变量被改变
        nums2.count();
        System.out.println("num2 2: " + longAccumulator.value());  // 3

        // 又调用了一次Action操作，广播变量所在的map又被执行了一次，所以累加器又被累加了一遍，就悲剧了
        nums2.count();
        System.out.println("num2 3: " + longAccumulator.value()); // 6

        // ------------------------------- 在transform算子中的正确使用 -------------------------------------------

        // 累加器不应该被重复使用，或者在合适的时候进行cache断开与之前Dataset的血缘关系，因为cache了就不必重复计算了
        longAccumulator.setValue(0);
        Dataset<Integer> nums3 = num1.map((MapFunction<Integer, Integer>) x -> {
            longAccumulator.add(1);
            return x;
        }, Encoders.INT()).cache(); // 注意这个地方进行了cache

        // 因为没有Action操作，nums.map并没有被执行，因此此时广播变量的值还是0
        System.out.println("num3 1: " + longAccumulator.value()); // 0

        // 调用一次action操作，广播变量被改变
        nums3.count();
        System.out.println("num3 2: " + longAccumulator.value());  // 3

        // 又调用了一次Action操作，因为前一次调用count时num3已经被cache，num2.map不会被再执行一遍，所以这里的值还是3
        nums3.count();
        System.out.println("num3 3: " + longAccumulator.value()); // 3

        // ------------------------------- 在action算子中的使用 -------------------------------------------
        longAccumulator.setValue(0);
        num1.foreach(x -> {
            longAccumulator.add(1);
        });
        // 因为是Action操作，会被立即执行所以打印的结果是符合预期的
        System.out.println("num4: " + longAccumulator.value()); // 3
    }
}
