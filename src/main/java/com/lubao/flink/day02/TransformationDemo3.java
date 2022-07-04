package com.lubao.flink.day02;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class TransformationDemo3 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        SingleOutputStreamOperator<Integer> res = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 != 0;
            }
        });

        SingleOutputStreamOperator<Integer> num2 = nums.filter(i -> i % 2 != 0).returns(Types.INT);

        res.print();

        env.execute("TransformationDemo3");
    }
}
