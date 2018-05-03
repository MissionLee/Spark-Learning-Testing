package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 03/04/18 15:54
  * @Description:
  * @Modified by:
  */
object TestGlom {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  val rdd2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic3.txt")

  def main(args: Array[String]): Unit = {
    val rdd3 = (rdd++ rdd2).cache()
    for(str <-  rdd3.collect()){
      println(str)
    }
    println("---------------------")
    for(arr <- rdd3.glom().collect()){
      for (str <- arr){
        println(str)
      }
      println("=======")
    }
    println("----------------------")
    rdd3.glom().foreach(r=>{
      for(str <- r){
        println(str)
      }
      println("--------------")
    })
  }
}
