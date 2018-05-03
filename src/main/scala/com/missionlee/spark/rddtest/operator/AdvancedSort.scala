package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  *
  */
class AdvancedSort[T]{
  //  def sum(hi: () => Unit): Unit = ???

  def sum(a : Any){println("11111")}
}
object AdvancedSort {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")



  def main(args: Array[String]): Unit = {
    //val uu = new User("a");
    //    def user = new User("helo")

    val rdd = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkbasic.txt")
    rdd.map(x=>new User(x)).map(x=>(x,x.name.length)).sortByKey(false).map(x=>x._1.name+":"+x._2.toString)foreach(println)

    rdd.map(x=>(x,null)).sortByKey()
    //    val adv = new AdvancedSort[String];
    //    val hi=()=>{print("222222")}
    //    adv.sum(hi)


  }

}
