package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 03/04/18 15:21
  * @Description:
  * @Modified by:
  */
object TestIntersection {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  val rdd2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic3.txt")

  def main(args: Array[String]): Unit = {
    val rdd3 = rdd.map(r=>new User(r))
    val rdd4= rdd2.map(r=>new User(r))
    val rdd5 = rdd3.intersection(rdd4)
    rdd5.map(_.getName())foreach(println)
  }

}
