package com.lubao.wordCount

import org.apache.flink.api.scala._

object BatchStreamWordCount02 {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val lines: DataSet[String] = env .readTextFile(args(0))

    val words: DataSet[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataSet[(String, Int)] = words.map((_, 1))

    val summed: AggregateDataSet[(String, Int)] = wordAndOne.groupBy(0).sum(1)


    summed.writeAsText(args(1))

    env.execute("BatchStreamWordCount")
  }

}
