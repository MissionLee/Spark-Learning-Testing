package com.missionlee.spark.sparkstreamingtest

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author: MissingLi
  *  a simple testFileStream
  */
object SparkStreamingHelloWorld {

  val ss = SparkSession.builder().master("local").appName("hello spark Streaming").getOrCreate()
  val sc = ss.sparkContext
  val ssc = new StreamingContext(sc,Seconds(1))

  def main(args: Array[String]): Unit = {
    val lines = ssc.textFileStream("file:///home/missingli/IdeaProjects/SparkLearn/streaming/logfile")
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x=>(x,1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
