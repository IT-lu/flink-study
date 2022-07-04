package com.lubao.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PrintSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        /**
         * PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
         * return addSink(printFunction).name("Print to Std. Out");
         *
         *
         *
         *
         * @Override
         *public void open(Configuration parameters) throws Exception {
         * 		super.open(parameters);
         * 		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
         * 		writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
         *}
         *
         * public void open(int subtaskIndex, int numParallelSubtasks) {
         * 		// get the target stream
         * 		stream = target == STD_OUT ? System.out : System.err;
         *
         * 		completedPrefix = sinkIdentifier;
         *
         * 		if (numParallelSubtasks > 1) {
         * 			if (!completedPrefix.isEmpty()) {
         * 				completedPrefix += ":";
         *          }
         * 			completedPrefix += (subtaskIndex + 1);
         * 		}
         * 		if (!completedPrefix.isEmpty()) {
         * 			completedPrefix += "> ";
         * 		}
         * 	}
         */
        lines.print("res");

        env.execute("PrintSink");
    }
}
