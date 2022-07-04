package com.lubao.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {


    public static void main(String[] args) throws Exception {

        //创建一个flink stream程序的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用env创建抽象的数据集DataStream
        //source
        DataStream<String> lines = env.socketTextStream("localhost", 8888);

        //调用DataStream上的方法Transformation
//        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String line, Collector<String> out) throws Exception {
//                //切分
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    //输出
//                    out.collect(word);
//                }
//            }
//        });
//
//        //将单词和一分组
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String word) throws Exception {
//                return Tuple2.of(word, 1);
//            }
//        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tp = Tuple2.of(word, 1);
                    out.collect(tp);
                }
            }
        });

        //将key相同的数据分到一个组里面
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumned = wordAndOne.keyBy(0).sum(1);

        //调用Sink
        sumned.print();


        //启动程序

        env.execute("StreamWord");

    }
}
