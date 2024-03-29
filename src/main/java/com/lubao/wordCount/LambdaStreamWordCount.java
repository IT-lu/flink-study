package com.lubao.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class LambdaStreamWordCount {

    public static void main(String[] args) throws Exception {
        //创建一个flink stream程序的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用env创建抽象的数据集DataStream
        //source
        DataStream<String> lines = env.socketTextStream("localhost", 8888);


//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
//            Arrays.stream(line.split(" ")).forEach(w -> {
//                out.collect(Tuple2.of(w, 1));
//            });
//        });
        SingleOutputStreamOperator<String> word = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = word.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        summed.print();

        env.execute("LambdaStreamWordCount");
    }
}
