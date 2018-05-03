package com.missionlee.spark.rddtest.operator

import java.util

import basic.TestFilter.sc
import org.apache.spark.sql.SparkSession

/**
  *
  */

object DistinctAndUnion {

  val ss =SparkSession.builder().appName("1").master("local").getOrCreate();
  val sc = ss.sparkContext

  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  sc.setLogLevel("error")

  def main(args: Array[String]): Unit = {
    val rdd2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
    val rdd3 = (rdd++rdd2).map(r=>{
      val list = new util.ArrayList[String]()
      list.add(r)
      if(2*Math.random()>1)
        list.add("limingshun")
      list
    }).cache()
/*    val rdd4 = rdd.union(rdd2)*/
//    println("rdd")
//    rdd.foreach(println)
//    println("rdd2")
//    rdd2.foreach(println)
    println("rdd3 = rdd1 ++rdd2")
    rdd3.foreach(println)
//    println("rdd4 = rdd.union(rdd2)")
    println(" distinct ------------")
    rdd3.map(r=>{
      var x ="";
      if(r.size()>1){
        x = r.get(0)+r.get(1)
      }else{
        x =r.get(0)
      }
      x
    }).foreach(println)
    println("---------------")
    rdd3.distinct().map(r=>{
      var x ="";
      if(r.size()>1){
        x = r.get(0)+r.get(1)
      }else{
        x =r.get(0)
      }
      x
    }).foreach(println)

    println("====================================")
    val rdd4=(rdd++rdd2).map(r=>new User(r))
    rdd4.map(r=>r.getName()).foreach(println)
    println("----------------------")
    rdd4.distinct().map(r=>r.getName()).foreach(println)
  }
}
