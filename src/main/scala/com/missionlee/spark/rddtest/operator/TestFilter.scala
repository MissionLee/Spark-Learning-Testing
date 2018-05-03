package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object TestFilter {
  val ss = SparkSession.builder().master("local").appName("1").getOrCreate()
  val sc = ss.sparkContext
  val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
  sc.setLogLevel("error")

  def main(args: Array[String]): Unit = {
    rdd.foreach(println)
    println("------------------")
    def oddFilter[T](strA : Array[T]):Boolean={
      val num = strA(0).toString.toInt
      return 1==num%2
    }
//   rdd.map(line=>line.split(",",-1)).filter(r=>1==(r(0).toInt%2)).map(_.mkString("_")).foreach(println)
    rdd.map(line=>line.split(",",-1)).filter(oddFilter).map(_.mkString("_")).foreach(println)
  }

}
