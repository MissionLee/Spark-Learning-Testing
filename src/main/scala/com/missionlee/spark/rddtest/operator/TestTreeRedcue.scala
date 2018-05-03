package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestTreeRedcue {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  def main(args: Array[String]): Unit = {

    println("----------reduce-----------")
    val c = sc.parallelize(1 to 10)
    val c2 = c.reduce(_+_)
    println(c2)
    println("-----------tree redcue-------")
    val c3 = c.treeReduce(_+_);
    println(c3)
  }
}
