package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 17/04/18 17:27
  * @Description:
  * @Modified by:
  */
object TestFold {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val x = List(1,2,3,4,5,6,7,8,9);
  val rdd0 =sc.parallelize(x)
  val rdd1 = sc.parallelize(x,1)
  val rdd2 = sc.parallelize(x,2)
  val a0 = rdd0.fold(100)((x,y)=>x+y)
  val a1 =rdd1.fold(100)((x,y)=>x+y)
  val a2 =rdd2.fold(100)((x,y)=>x+y)


  def main(args: Array[String]): Unit = {
    println("a0:"+a0)
    println("a1:" + a1)
    print("a2:" + a2)
  }
}
