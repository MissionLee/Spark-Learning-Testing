package com.missionlee.spark.rddtest.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  */
class Wrap(x : String){
  val name = x;
  def sayName(): String ={
    println(x)
    "123"
  }

  def getName(): String = {
    this.x
  }
}

class Transform {

}

object Transform{
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  def run(): Unit ={
    val list = rdd.map(r => List(new Wrap(r)))
    println("sayName")
    val saylist =list.map(_(0).sayName())
    println("sayName + collect")
    saylist.collect()
    println("sayName + println")
    saylist.foreach(println)
    println()
    println("getName + println")
    list.map(_(0).getName()).foreach(println)
    println("---------")
    val x =rdd.map(r=>r.split(",",-1)).map(r=>(r(0),r)).sortByKey(false).saveAsTextFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic2.txt")

  }

  def main(args: Array[String]): Unit = {
    run()
  }
}
