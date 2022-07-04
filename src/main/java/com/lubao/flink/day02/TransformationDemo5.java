package com.lubao.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationDemo5 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<WordCounts> wordAndOne = words.map(new MapFunction<String, WordCounts>() {
            @Override
            public WordCounts map(String value) throws Exception {
//                return new WordCounts(value,1L);
                return WordCounts.of(value,1L);
            }
        });


        //指定字段
        KeyedStream<WordCounts, Tuple> keyed = wordAndOne.keyBy("word");

        //聚合
        keyed.sum("counts");

        keyed.print();

        env.execute("TransformationDemo5");
    }
}
