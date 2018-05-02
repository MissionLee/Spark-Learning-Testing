package com.missionlee.spark.structuredstreamingtest

import org.apache.spark.sql.SparkSession


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
    //spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val lines=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "missionlee")
      .load()
    val numbers = lines.selectExpr("CAST(value AS STRING)").as[String].flatMap(_.split(" "))//.map(_.toInt)
    val sum = numbers.groupBy().count()

    val quer = sum.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    quer.awaitTermination()

  }
}
