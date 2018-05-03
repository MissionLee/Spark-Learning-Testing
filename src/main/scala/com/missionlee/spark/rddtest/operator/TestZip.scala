package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestZip {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val a1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip1.txt")
  val a2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip2.txt")
  val b1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip3.txt")
  val b2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip4.txt")

  def main(args: Array[String]): Unit = {
    val a = a1++a2
//    val b = b1++b2
    val b = b2++b1
    val ziprdd = a.zip(b)
    ziprdd.foreach(a=>println(a._1+""+a._2))
  }
}
