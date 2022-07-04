package com.lubao.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationDemo6 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


//        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
//            @Override
//            public Tuple3<String, String, Double> map(String value) throws Exception {
//
//                String[] fields = value.split(" ");
//                String province = fields[0];
//                String city = fields[1];
//                double money = Double.parseDouble(fields[2]);
//                return Tuple3.of(province, city, money);
//            }
//        });

        SingleOutputStreamOperator<OrderBean> provinceCityAndMoney = lines.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                String[] fields = value.split(" ");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return OrderBean.of(province, city, money);
            }
        });


        //指定字段
//        KeyedStream<Tuple3<String, String, Double>, Tuple> keyed = provinceCityAndMoney.keyBy(0, 1);
        KeyedStream<OrderBean, Tuple> keyed = provinceCityAndMoney.keyBy("province", "city");
//        SingleOutputStreamOperator<Tuple3<String, String, Double> > summed = keyed.sum(2);
        SingleOutputStreamOperator<OrderBean> summed = keyed.sum("money");

        summed.print();

        env.execute("TransformationDemo6");
    }
}
