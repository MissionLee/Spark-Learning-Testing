package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestReduce {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val a1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip1.txt")
  val a2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip2.txt")
  val b1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip3.txt")
  val b2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip4.txt")

  def main(args: Array[String]): Unit = {
    println("---------String-----")
    val rdd1 = a1 ;
    val rdd2 =rdd1.reduce(_+_)
    println(rdd2)
    println("----------Int-----------")
    val c = sc.parallelize(1 to 10)
    val c2 = c.reduce(_+_)
    println(c2)
    println("----------Int with partition--------")
    val c3 = c++c++c;
    val c4 =c3.reduce(_+_)
    println(c4)
    println("----------find the largest number-----------")
    val c5 = c.reduce((x,y)=>if(x>y) x else y)
    println(c5)
  }
}
