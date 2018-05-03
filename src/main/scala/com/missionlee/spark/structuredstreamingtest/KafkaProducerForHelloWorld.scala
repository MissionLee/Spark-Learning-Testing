package com.missionlee.spark.structuredstreamingtest

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util
import java.util.Date
/**
  *
  */
object KafkaProducerForHelloWorld {
  def main(args: Array[String]): Unit = {
    val Array(brokers,topic,messagesPerSec,wordsPerMessage)=Array("localhost:9092","missionlee","1","1")

    //zookeeper connection
    val props = new util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers) //
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    //send some messages
    while(true){
      // 创建一个 几行的，每一行是几个数字的字符串
      (1 to messagesPerSec.toInt).foreach { messageNum =>
//        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
//          .mkString(" ")
        val str = new Date().getTime.toString; //I want to test how fast structured streaming can deal with
                                       // the data ,so I just send the time string to kafka
        //print(str)
        //println()
        // 把这个字符串封装成 一条记录
        val message = new ProducerRecord[String, String](topic, null, str)
        // 发送这条记录
        producer.send(message)
      }
      Thread.sleep(10) // sleep 的时间 根据需求 调整
    }
  }
}
