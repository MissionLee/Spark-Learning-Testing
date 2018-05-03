package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestGroupBy {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")

  def main(args: Array[String]): Unit = {
    val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
    val rdd2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic3.txt")
    val rdd3 = (rdd++rdd2).groupBy(r=>{
      r.split(",",-1)(0)
    }).foreach(r=>{
      println("-------------------")
      println(r._1)
      for(str <- r._2){
        println(str)
      }
    })
    val rdd4 = (rdd++rdd2).map(r=>(  r.split(",",-1)(0), r   )).groupByKey().foreach(r=>{
      println("-------------------")
      println(r._1)
      for(str <- r._2){
        println(str)
      }
    })
  }
}
