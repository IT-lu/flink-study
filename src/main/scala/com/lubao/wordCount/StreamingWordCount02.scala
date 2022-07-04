package com.lubao.wordCount

import org.apache.flink.streaming.api.scala._

object StreamingWordCount02 {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)


    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

    val sumned: DataStream[(String, Int)] = wordAndOne.keyBy(0).sum(1)


    //Sink
    sumned.print()



    env.execute("StreamWordCount")
  }
}
