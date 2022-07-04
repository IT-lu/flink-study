package com.lubao.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblingWindowAll {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });


        //不分组 整体当成一个组 5s滚动一次
        AllWindowedStream<Integer, TimeWindow> window = nums.timeWindowAll(Time.seconds(5));

        //在窗口聚合
        SingleOutputStreamOperator<Integer> summed = window.sum(0);


        summed.print();

        env.execute("TumblingWindowAll");


    }
}
