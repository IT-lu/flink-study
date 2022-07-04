package com.lubao.flink.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;

/**
 *可以多个并行的Source
 */
public class SourceDemo2 {

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
//        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);


        /**
         * public abstract class SplittableIterator<T> implements Iterator<T>, Serializable {
         * }
         *
         * public class NumberSequenceIterator extends SplittableIterator<Long> {
         * }
         */
//        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);

//        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), TypeInformation.of(Long.TYPE));
        DataStreamSource<Long> nums = env.generateSequence(1, 100);

        int parallelism = nums.getParallelism();

        System.out.println("++++++++++>" + parallelism);

        SingleOutputStreamOperator<Long> filtered = nums.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).setParallelism(3);

        int parallelism1 = filtered.getParallelism();

        System.out.println("--------->" + parallelism1);

        filtered.print();

        env.execute("SourceDemo1");

    }
}
