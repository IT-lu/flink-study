package com.lubao.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class AddSinkDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.addSink(new RichSinkFunction<String>() {

            @Override
            public void invoke(String value, Context context) throws Exception {

                int index = getIterationRuntimeContext().getIndexOfThisSubtask();
                System.out.println(index + " > " + value);
            }
        });


        env.execute("PrintSink");
    }
}
