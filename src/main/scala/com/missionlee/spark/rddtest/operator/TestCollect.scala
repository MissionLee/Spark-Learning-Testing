package com.missionlee.spark.rddtest.operator

import org.apache.spark.sql.SparkSession

/**
  * @author :
  */
object TestCollect {
  val ss = SparkSession.builder().master("local").appName("basic").getOrCreate()
  val sc = ss.sparkContext
  sc.setLogLevel("error")
  val a1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip1.txt")
  val a2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip2.txt")
  val b1 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip3.txt")
  val b2 = sc.textFile("/home/missingli/IdeaProjects/SparkLearn/src/main/resources/sparkzip4.txt")

  def main(args: Array[String]): Unit = {
    val rdd4 = sc.parallelize(List(1, 2, 3))
    val rdd = a1 ++ a2 ++ b1 ++ b2
    val array = rdd.collect()
//    for (str <- array) {
//      println(str)
//    }
//    val x = List(1, 2, 3, "abc")
//    val y = x.collect {
//      case i: Int  => i
//      case i: String => "find a string"
//    }
//    for (i <- y) {
//      println(i)
//    }


    // PartialFunction with List
//    val x = List("A1","A2","A3","B4",5,6,7)
//    val f = new PartialFunction[String, String] {
//      override def isDefinedAt(x: String): Boolean = if (x.startsWith("A")) true else false
//
//      override def apply(v1: String): String = v1 + ";"
//    }
//    val f2 = new PartialFunction[Any, String] {
//      // in fact we use isDefinedAt with :  isInstanceOf[...]
//      override def isDefinedAt(x: Any): Boolean = if (x.toString.startsWith("A")) true else false
//
//      override def apply(v1: Any): String = v1.toString
//    }
////    val x2 = x.collect(f) can note resolve reference with such signature
//    println("----------x3--------------")
//    val x3 = x.collect(f2)
//    for(str <-x3){
//      println(str)
//    }


    val array2 = rdd.collect { case a: String => {
      if (a.startsWith("A")) a
    }
    }
    val array3 = rdd.collect(new MyParitialFunction)

    println(array2.collect().length)
    println(" array2")
    for (str <- array2) {
      println(str.getClass)
      println(str)
    }
    println("array 3")
    for (str <- array3) {
      println(str)
    }
  }
}
