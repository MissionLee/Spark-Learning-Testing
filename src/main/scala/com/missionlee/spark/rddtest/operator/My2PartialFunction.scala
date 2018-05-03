package com.missionlee.spark.rddtest.operator

/**
  * @author: MissingLi
  * @date: 09/04/18 17:05
  * @Description:
  * @Modified by:
  */
class My2PartialFunction  extends PartialFunction [Any,Int]{
  override def isDefinedAt(x: Any): Boolean = ???

  override def apply(v1: Any): Int = ???

}
