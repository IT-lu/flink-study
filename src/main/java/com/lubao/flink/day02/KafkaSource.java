package com.lubao.flink.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka中读取数据的dataSource 可以是并行的Source 并且可以实现ExactlyOne
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //KafkaSource
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.setProperty("bootstrap.servers","localhost:9092");
        //指定组ID
        properties.setProperty("group.id","gwc10");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset","earliest");
        //Kafka的消费者不自动提交偏移量
//        properties.setProperty("enable.auto.commit","false");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10", //topic 消费主题
                new SimpleStringSchema(), //序列化方式
                properties
        );

        DataStreamSource<String> lines = env.addSource(kafkaSource);

        lines.print();



        env.execute("kafkaSource");

    }
}
