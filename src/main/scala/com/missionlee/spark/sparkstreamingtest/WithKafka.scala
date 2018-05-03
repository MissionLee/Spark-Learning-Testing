package com.missionlee.spark.sparkstreamingtest

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 19/04/18 20:16
  * @Description:
  * @Modified by:
  */
object WithKafka {
  val ss = SparkSession.builder().appName("kafka test").master("local[*]").getOrCreate()
  val sc = ss.sparkContext
//  val ssc = new
}
