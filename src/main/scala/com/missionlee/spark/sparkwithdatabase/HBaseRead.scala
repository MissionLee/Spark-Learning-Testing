package com.missionlee.spark.sparkwithdatabase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建一个student表，我们要在这个表中录入如下数据：
  **
+------+----------+--------+------+
  *| id   | name     | gender | age  |
  *+------+----------+--------+------+
  *|    1 | Xueqian  | F      |   23 |
  *|    2 | Weiliang | M      |   24 |
  *+------+----------+--------+------+
  *我们可以在hbase shell中使用下面命令创建：
  **
 hbase>  create 'student','info'
  *hbase
  *你可以发现，我们在创建student表的create命令中，命令后面首先跟上表名称’student’，然后，再跟上列族名称’info’，这个列族’info’中包含三个列’name’,’gender’,’age’。你会发现，好像没有’id’字段，这是因为HBase的表中会有一个系统默认的属性作为行键，无需自行创建，默认把put命令操作中跟在表名后的第一个字段作为行健。
  *创建完“student”表后，可通过describe命令查看“student”表的基本信息：
  **
 hbase> describe 'student'
  *hbase
  *下面，我们要把student表的两个样例记录输入到student表中。但是，HBase是列族数据库，原理和关系数据库不同，操作方法也不同。如果要了解HBase的技术原理和使用方法，可以参考厦门大学数据库实验室的在线课程《HBase数据库》。
  *如果没有时间学习HBase数据库细节知识，也可以直接按照下面的内容跟着操作就可以了。
  *HBase中用put命令添加数据，注意：一次只能为一个表的一行数据的一个列（也就是一个单元格，单元格是HBase中的概念）添加一个数据，所以直接用shell命令插入数据效率很低，在实际应用中，一般都是利用编程操作数据。因为这里只要插入两条学生记录，所以，我们可以用shell命令手工插入。
  **
 //首先录入student表的第一个学生记录
  *hbase> put 'student','1','info:name','Xueqian'
  *hbase> put 'student','1','info:gender','F'
  *hbase> put 'student','1','info:age','23'
/然后录入student表的第二个学生记录
  *hbase> put 'student','2','info:name','Weiliang'
  *hbase> put 'student','2','info:gender','M'
  *hbase> put 'student','2','info:age','24'
  *hbase
  *数据录入结束后，可以用下面命令查看刚才已经录入的数据：
  **
 //如果每次只查看一行，就用下面命令
  *hbase> get 'student','1'
/如果每次查看全部数据，就用下面命令
  *hbase> scan 'student'
  */
object HBaseRead {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    println(conf.get("hbase.rootdir"))
    println(conf.get("hbase.master.port"))
    println(conf.get("io.storefile.bloom.block.size"))
    /**
      * about the hbase-default.xml
      *
      * this file cannot be found in the file system of the hbase
      * but i find it in it's source code in hbase-common/resource =>means that : in the .jar file
      *
      * in that file , there is hbase.master.port and so on
      * */
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("spark_hbase"))
    sc.setLogLevel("error")
    // set parameters about hbase

    // attentions !  we need to import
    //           org.apache.hadoop.hbase.mapreduce.TableInputFormat
    // instead of
    //           org.apache.hadoop.hbase.mapred.TableInputFormat
    // in this step
    conf.set(TableInputFormat.INPUT_TABLE,"student")
    val stdRdd = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stdRdd.count()
    println("students rdd count:"+count)
    // print the stdrdd
    stdRdd.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    })
  }

}
