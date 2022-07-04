package com.lubao.flink.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 *并行度为1的Source
 */
public class SourceDemo1 {

    public static void main(String[] args) throws Exception {

        //实时计算 创建一个实现的执行环境
        /**
         * ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         * 		if (env instanceof ContextEnvironment) {
         * 			return new StreamContextEnvironment((ContextEnvironment) env);
         *                } else if (env instanceof OptimizerPlanEnvironment || env instanceof PreviewPlanEnvironment) {
         * 			return new StreamPlanEnvironment(env);
         *        } else {
         * 			return createLocalEnvironment();
         *        }
         *
         * public static LocalStreamEnvironment createLocalEnvironment() {
         * 		return createLocalEnvironment(defaultLocalParallelism);
         *        }
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建抽象的数据集 Source
        /**
         * public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {
         * }
         *
         * public class SingleOutputStreamOperator<T> extends DataStream<T> {
         * }
         */
        //DataStream是一个抽象的数据集

        /**
         * /**
         *  * A DataStream represents a stream of elements of the same type. A DataStream
         *  * can be transformed into another DataStream by applying a transformation as
         *  * for example:
         *  * <ul>
         *  * <li>{@link DataStream#map}
         *  * <li>{@link DataStream#filter}
         *  * </ul>
         *  *
         *  * @param <T> The type of the elements in this stream.
         */
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);


        //设置DataSource的并行度
//        socketTextStream.setParallelism(4);
        int parallelism2 = socketTextStream.getParallelism();


        System.out.println("++++++++++>" + parallelism2);
        //将客户端的集合转化成一个抽象的数据集
        //fromElements是一个有界数据流 数据处理完成 程序会自动退出
//        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //并行度为1的Source
        DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        //获取这个DataStream的并行度
        int parallelism = nums.getParallelism();

        System.out.println("==========>" + parallelism);
        SingleOutputStreamOperator<Integer> filtered = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });

        int parallelism1 = filtered.getParallelism();

        System.out.println("--------->" + parallelism1);

        filtered.print();

        env.execute("SourceDemo1");

    }
}
