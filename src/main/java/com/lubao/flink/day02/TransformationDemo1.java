package com.lubao.flink.day02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationDemo1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);

        //做映射
        SingleOutputStreamOperator<Integer> res = nums.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return 2 * value;
            }
        });


        SingleOutputStreamOperator<Integer> res2 = nums.map(i -> i * 2).returns(Types.INT);

        //传入功能更加强大的RichMapFunction
        nums.map(new RichMapFunction<Integer, Integer>() {

            //open 在构造方法之后， map方法执行之前 执行一次 Configuration拿到全局配置
            //用来初始化连接 或者初始化或者恢复历史state
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            //销毁之前 执行一次 通常是做资源的释放
            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return null;
            }
        });
        res.print();

        env.execute("TransformationDemo1");
    }
}
