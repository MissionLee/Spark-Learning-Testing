package com.missionlee.spark.sparkwithdatabase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object HBaseWrite {
  // saprk conf
  val sconf = new SparkConf().setAppName("sparkReadHbase").setMaster("local");
  // sc
  val sc  = new SparkContext(sconf);
  val tablename = "student";

  /**
    * two ways to  set parameter to write to hbase
    * 1. with sc.hadoopConfiguration
    * 2. create a new HbaseConfiguration
    */

  // 1 : use  sc.hadoopConfiguration
  sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tablename);
  sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
  // 2 : use HbaseConfiguration
  //val conf = HBaseConfiguration.create()
  //conf.set(TableOutputFormat.OUTPUT_TABLE,"student")
  //conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

  //println(conf.get("hbase.rootdir"))
  // get a job and parameters
  val job = new Job(sc.hadoopConfiguration)
  //val job = new Job(conf)
  job.setOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setOutputValueClass(classOf[Result])
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
  // get RDD
  val indataRDD = sc.makeRDD(Array("13,Rongcheng,M,26","14,Guanhua,M,27")) //构建两行记录
  val rdd = indataRDD.map(_.split(',')).map{arr=>{
    val put = new Put(Bytes.toBytes(arr(0))) //行健的值
    put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))  //info:name列的值
    put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))  //info:gender列的值
    put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3)))  //info:age列的值
    (new ImmutableBytesWritable, put)
  }}
  // put the rdd into hbase
  rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
}
}
