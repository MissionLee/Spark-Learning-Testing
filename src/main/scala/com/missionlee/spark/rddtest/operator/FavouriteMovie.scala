package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
object FavouriteMovie {
  val ss = SparkSession.builder().appName("1").master("local").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")

  def main(args: Array[String]): Unit = {
    val rddFav = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/favourite.txt")
    val rddMov = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/movie.txt")
    val rddmid = rddFav.map(r=>r.split(",",-1)).filter(r=>r(1)=="uid2").map(r=>(r(2),r))
    rddmid.map(r=>r._1).foreach(println)
    println("---------------")
   val rddresult = rddMov.map(_.split(",",-1)).map(r=>(r(0),r)).join(rddmid)
     .foreach(r=>{
     println("uid1,movieId:"+r._1+",movieName:"+r._2._1(1))
   })
  }
}
