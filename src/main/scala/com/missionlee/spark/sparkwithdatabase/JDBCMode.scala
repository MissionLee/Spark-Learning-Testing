package com.missionlee.spark.sparkwithdatabase

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @author: MissingLi
  * @date: 03/05/18 20:13
  * @Description:
  * @Modified by:
  */
object JDBCMode {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("app1")
    val sc = new SparkContext(conf)
    val spk = SparkSession.builder.getOrCreate
    val jdbcDF = spk.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/learning")
      .option("driver", "org.mariadb.jdbc.Driver")
      .option("dbtable", "user").option("user", "root").option("password", "shunzhiai").load.show

    // write to mariadb  learning-user(uname,pwd)
    // create some rdd data
    val studentRDD = spk.sparkContext.parallelize(Array("spark 123", "hadoop 456")).map(_.split(" "));
    // create schema
    val schema = StructType(List(
      StructField("uname", StringType, true),
      StructField("pwd", StringType, true))
    )
    // create row
    val rowRDD = studentRDD.map(p => Row(p(0).trim, p(1).trim));
    // link  row with schema
    val studentDF = spk.createDataFrame(rowRDD, schema)
    // create jdbc link [in another way]
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "shunzhiai")
    prop.put("driver", "org.mariadb.jdbc.Driver")
    // append the data to mariadb
    studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/learning", "user", prop);

  }
}
