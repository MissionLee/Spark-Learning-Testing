package com.missionlee.spark.rddtest.operator

/**
  *
  */
class User(val name:String) extends Serializable {

  def getName(): String ={
    name
  }

  override def hashCode(): Int = {

    var h:Int = 0;
    if (h == 0 && name.length > 0) {
      val ar: Array[Char] = name.toCharArray;
      for( c <- ar){
        h=31*h+c
      }
    }
    h
  }
  override def equals(obj: Any): Boolean = {
    println(" here equals ")
    this.hashCode() == obj.hashCode()
  }
  def compare(obj :User): Int ={
    return this.name.length - obj.name.length
  }

}
object User{
  //
  def myOrd =new Ordering[User] {
    override def compare(x: User, y: User): Int = {
      println(" implicit userOrder")
      x.compare(y)
    }
  }
  implicit def userOrder =myOrd

  def main(args: Array[String]): Unit = {
    println()
  }
}
