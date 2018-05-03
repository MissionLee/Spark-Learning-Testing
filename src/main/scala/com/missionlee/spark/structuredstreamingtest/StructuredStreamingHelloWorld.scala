package com.missionlee.spark.structuredstreamingtest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
  *
  */
object StructuredStreamingHelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StructuredStreamingHelloWorld")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "missionlee")
      .load()
    val time = lines.selectExpr("CAST(value AS STRING)") //.as[String].flatMap(_.split(" "))//.map(_.toInt)
    //    val sum = time.foreach(_=>{
    //      print(_)
    //    })
    //time.groupBy(window($"","",""),$"")

    val quer = time.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.Continuous("10 milliseconds"))
      .start()
    quer.awaitTermination()


  }
}
