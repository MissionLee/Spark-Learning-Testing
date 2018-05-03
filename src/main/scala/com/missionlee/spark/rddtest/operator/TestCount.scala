package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 18/04/18 09:42
  * @Description:
  * @Modified by:
  */
object TestCount {
  val ss = SparkSession.builder().appName("123").master("local").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  def main(args: Array[String]): Unit = {
//    val rdd = sc.parallelize(1 to 1000000000,100).cache()
//    val result1 = rdd.countApprox(2,0.05)
//    val x =result1.getFinalValue()
//
//    println(result1)
//    println(x)
//
//    val result2 = rdd.countApprox(10000,0.05)
//    println(result2)


//    val rdd = sc.parallelize(Range(1,10,1))
//    val rdd2 = sc.parallelize(Range(1,10,2))
//    val rdd3 = rdd++rdd2;
//    rdd3.countByValue().foreach(println)

    val rdd = (sc.parallelize(Range(1,1000),10)++sc.parallelize(Range(1,1000,2),10)).cache()
    val x =rdd.countApproxDistinct(0.1)
    val y = rdd.countApproxDistinct(0.2)
    val z = rdd.countApproxDistinct(0.6)
    val num = rdd.countApproxDistinct(1)
    val num2 = rdd.countApproxDistinct(100)

    println(x)
    println(y)
    println(z)
    println(num)
    println(num2)
    println(rdd.countApproxDistinct(0.01))
  }

}
