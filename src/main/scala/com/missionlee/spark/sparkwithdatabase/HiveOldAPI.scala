package com.missionlee.spark.sparkwithdatabase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author: MissingLi
  * @date: 03/05/18 20:13
  * @Description:
  * @Modified by:
  */
object HiveOldAPI {
  def main(args: Array[String]): Unit = {
    /***
      * --- open one shell
      * su hadoop
      * start-dfs.sh
      *jps
      *   - 28464 SecondaryNameNode
      *   - 28049 NameNode
      *   - 28613 Jps
      *   - 28214 DataNode
      * hiveserver2 [this shell will start to show the server information]
      *
      *------ open new shell
      *
      * beeline
      *  !connect jdbc:hive2://localhost:1000
      *  // todo : test whether hive is working or not !
      *
      * */
    // TODO: this one is OK now!
    val conf = new SparkConf().setMaster("local").setAppName("app3");
    val sc = new SparkContext(conf);
    val warehouseLocation = "file:///tmp/spark-warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport().getOrCreate()
    //spark.conf.set()
    import spark.implicits._
    import spark.sql
    //val hivecontext = new HiveContext(sc);
    val studntdf = sql("select * from lms.student").toDF()
    //studntdf.registerTempTable("mystudent")
    studntdf.createOrReplaceTempView("mystudent")
    sql("select name from mystudent").show()

  }
}
