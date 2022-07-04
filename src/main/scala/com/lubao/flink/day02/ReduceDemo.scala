package com.lubao.flink.day02

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object ReduceDemo {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)


    val keyed: KeyedStream[(String, Int), Tuple] = lines.flatMap(_.split(" ")).map((_, 1)).keyBy(0)

    val reduced: DataStream[(String, Int)] = keyed.reduce((m, n) => {
      val key = m._1
      val v1 = m._2
      val v2 = n._2
      (key, v1 + v2)
    })

    //Sink
    reduced.print()



    env.execute("ReduceDemo")
  }
}
