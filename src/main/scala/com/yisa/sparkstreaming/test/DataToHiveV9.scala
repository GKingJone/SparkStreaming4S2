//package com.yisa.sparkstreaming.engine
//
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark._
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.streaming.kafka._
//
//import com.google.gson.Gson
//import com.google.gson.Gson
//import com.google.gson.reflect.TypeToken
//import com.google.gson.reflect.TypeToken
//import com.yisa.sparkstreaming.model.PassInfo2
//import com.yisa.sparkstreaming.model.PassinfoForHive
//import com.yisa.sparkstreaming.model.PassinfoForHive
//import com.yisa.sparkstreaming.source.Config
//import com.yisa.sparkstreaming.source.SparkSessionSingletonModel
//import java.util.Calendar
//import com.yisa.sparkstreaming.model.PassinfoForHive3
//import org.apache.kafka.clients.consumer.KafkaConsumer
//import java.util.ArrayList
//import java.util.Properties
//import com.yisa.wifi.zookeeper.ZookeeperUtil
//
///**
// * @author liliwei
// * @date  2016年9月9日
// *
// */
//object DataToHive9 {
// 
//  def main(args: Array[String]) = {
//
//    val zkHostPort = args(0);
//    val kafkagroupid = args(1);
//    val zkUtil = new ZookeeperUtil()
//    val configs = zkUtil.getAllConfig(zkHostPort, "spark_engine", false)
//    val kafka = configs.get("KafkaHostport")
//    val topic = configs.get("KAFKA_TOPIC")
//
//    val sparkSession = SparkSession
//      .builder()
//      .appName("DataToHiveUseKafka")
//      .getOrCreate()
//
//    import sparkSession.implicits._
//
//    var props = new Properties()
//    props.put("bootstrap.servers", kafka)
//    props.put("group.id", kafkagroupid)
//    props.put("enable.auto.commit", "false")
//    props.put("auto.commit.interval.ms", "1000")
//    //    props.put("heartbeat.interval.ms", "59999")
//    props.put("auto.offset.reset", "latest")
//    //    props.put("session.timeout.ms", "60000")
//    //    props.put("request.timeout.ms", "60001")
//    //    props.put("group.max.session.timeout.ms", "600000")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    var consumer = new KafkaConsumer(props)
//    //kafka topic name 
//    var topics = new ArrayList[String]()
//    topics.add(topic)
//    consumer.subscribe(topics)
//
//    var preHour = new Date().getTime()
//
//    // read kafka data
//    while (true) {
//      var records = consumer.poll(100)
//      var rei = records.iterator()
//      var count = 0
//      while (rei.hasNext()) {
//        //强制让kafka consumer提交同步，以防止后续任务耗时时间过长
//        try {
//          consumer.commitAsync();
//        } catch {
//          case e: Exception => {
//            println("kafka consumer commitAsync wrong!!!")
//            println(e.getMessage)
//            Thread.sleep(300)
//            consumer.commitSync();
//          }
//        }
//        var record = rei.next()
//        count = count + 1
//      }
//      println(count)
//    }
//  }
//}
//
