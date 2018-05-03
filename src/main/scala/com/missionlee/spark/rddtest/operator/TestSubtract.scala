package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestSubtract {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val a1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip1.txt")
  val a2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip2.txt")
  val b1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip3.txt")
  val b2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip4.txt")

  def main(args: Array[String]): Unit = {
    val rdd1 = a1++a2 ;
    rdd1.foreach(println)
    println(" subtract ")
    rdd1.subtract(a2).foreach(println)
    println("--------------------")
    val rdd2 = a1++a2++a2
    rdd2.foreach(println)
    println("subtract")
    rdd2.subtract(a2).foreach(println)
  }
}
