package com.lubao.flink.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 对于同一个job，不同Task【阶段】的subTask可以在同一个资源槽中（默认的方式，Pipeline),前提是默认的所有task的共享资源槽的名字是相同的
 */
public class StreamWordCountSharingGroupTest {


    public static void main(String[] args) throws Exception {

        //创建一个flink stream程序的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用env创建抽象的数据集DataStream
        //source
        DataStream<String> lines = env.socketTextStream("localhost", 8888);

        //调用DataStream上的方法Transformation
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                //切分
                String[] words = line.split(" ");
                for (String word : words) {
                    //输出
                    out.collect(word);
                }
            }
        }).slotSharingGroup("doit");

        SingleOutputStreamOperator<String> filtered = words.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                return value.startsWith("h");
            }
        });
        //.disableChaining(); //从该算子开始到该算子结束 单独划分处理  划分出来一个task 跟其他算子不再有Operator Chain
        //.startNewChain(); //从该算子开始 开启一个新的链 从这个算子之前 发生redistributing


        //将单词和一分组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //启动程序

        wordAndOne.print();

        env.execute("StreamWord");

    }
}
