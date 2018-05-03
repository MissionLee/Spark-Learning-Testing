package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 17/04/18 17:42
  * @Description:
  * @Modified by:
  */
object TestAggregate {
  val ss = SparkSession.builder().master("local").appName("123").getOrCreate()
  val sc = ss.sparkContext

  def main(args: Array[String]): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val result = rdd.aggregate(0)((x, y) => x + y, (x, y) => x + y);
    println(result) //45

    val result2 = rdd.aggregate((0, 0))(
      (x, y) => {
        println("x:"+x)
        (x._1 + 1, x._2 + y)}
    ,
      (a, b) => (a._1 + b._1, a._2 + b._2))
    println(result2) //(9,45)

    println(rdd.count())
    }


}
