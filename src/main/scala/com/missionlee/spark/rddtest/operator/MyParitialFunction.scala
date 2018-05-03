package com.missionlee.spark.rddtest.operator

import java.util

/**
  * @author: MissingLi
  * @date: 09/04/18 14:55
  * @Description:
  * @Modified by:
  */
class MyParitialFunction extends Serializable   with PartialFunction [Any,String]{
  override def isDefinedAt(x: Any): Boolean = if(x.toString.startsWith("A")) true else false

  override def apply(v1: Any): String = v1.toString

  def main(args: Array[String]): Unit = {
//    util.Arrays.toString()
  }
}
