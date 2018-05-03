package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 03/04/18 14:06
  * @Description:
  * @Modified by:
  */
object TestSortBy {

    val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("error")
    val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")

    def main(args: Array[String]): Unit = {
      (rdd++rdd++rdd++rdd++rdd).sortBy(r=>{
        r.split(",",-1).length
      },false,2).foreach(println)
    }
}
