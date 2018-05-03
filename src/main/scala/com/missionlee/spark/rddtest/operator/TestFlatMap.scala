package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession
import shapeless.ops.nat.GT.>

import scala.collection.immutable.HashSet

/**
  *
  */
object TestFlatMap {
  val ss = SparkSession.builder().master("local").appName("1").getOrCreate()
  val sc = ss.sparkContext
  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  sc.setLogLevel("error")

  def main(args: Array[String]): Unit = {
    rdd.foreach(println)
    val flatrdd = rdd.flatMap(line=>line.split(",",-1))
    println("------------------")
    flatrdd.foreach(println)
    println("-------------------")
    def myfuc(line:String ):Set[String]={
      val list0 = line.split(",",-1)
      var hset = new HashSet[String]
      hset =hset + list0(0)
      hset =hset + list0(1)
      hset
    }
    val flatrdd2 = rdd.flatMap(myfuc)
    flatrdd2.foreach(println)
  }
}
