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
//import scala.util.parsing.json.JSON
//
///**
//* @author gaoxl
//* @date  2016年9月9日 
//* 
//* json测试
//*/
//object DataToHive5 {
//
//  def main(args: Array[String]) = {
//    
//    Config.initConfig();
//    
//    val Group_ID = "jsonceshi0001"
//    
//    println("Group_ID : " + Group_ID)
//    
//    var sprakConf = new SparkConf().setAppName("DataToHive8")
//    var sc = new SparkContext(sprakConf)
//    val streamingContext = new StreamingContext(sc, Minutes(Config.READ_KAFKA_DATA_TIME))
//
////    val kafkaStream = KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 1), StorageLevel.MEMORY_AND_DISK_SER)
//
//    val numDStreams = 10
//    val kafkaStream = (1 to numDStreams).map { _ =>
//      KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 5), StorageLevel.MEMORY_AND_DISK_SER)
//    }
//    
//    val unionStream = streamingContext.union(kafkaStream)
//    
//    var data = unionStream.filter(!_._2.toString().equals(""))
//    
//    println("kafkaStream : " + unionStream.count())
////      var aa = data.map(line_data => {
//  
//        var line = line_data._2
//  
////        val gson = new Gson
////        val mapType = new TypeToken[PassInfo2] {}.getType
////        var passInfo2 = gson.fromJson[PassInfo2](line, mapType)
//        
//        var jsonData = JSON.parseFull(line)  
//        var mapJson: Map[String, Any] = null
//        jsonData match {  
//          case Some(map: Map[String, Any]) => mapJson = map
//          case None => println("Parsing failed")  
//          case other => println("Unknown data structure: " + other)  
//        }  
//        
//        if(mapJson != null){
//        
//        }
//        
//        var inputBean = PassinfoForHive3.apply(
//          mapJson.get("id").get.toString(),
//          mapJson.get("plateNumber").get.toString(),
//          getTimestamp(mapJson.get("captureTime").get.toString()),
//          mapJson.get("directionId").get.toString().toInt,
//          mapJson.get("colorId").get.toString().toInt,
//          mapJson.get("modelId").get.toString().toInt,
//          mapJson.get("brandId").get.toString().toInt,
//          mapJson.get("levelId").get.toString().toInt,
//          mapJson.get("yearId").get.toString().toInt,
//          mapJson.get("feature").get.toString(),
//          mapJson.get("locationuuid").get.toString(),
//          mapJson.get("lastCaptured").get.toString().toInt,
//          mapJson.get("isSheltered").get.toString().toInt,
//          mapJson.get("createTime").get.toString().toLong,
//          getDateId(mapJson.get("captureTime").get.toString())
//          )
//  
//        inputBean
//  
//      })
//  
//      aa.foreachRDD { rdd =>
//        {
//          val warehouseLocation = Config.SPARK_WARE_HOUSE_LOCATION
//          val spark =  SparkSessionSingletonModel.getInstance(warehouseLocation)
//          
//          import spark.implicits._
//  
//          // Convert RDD[String] to DataFrame
//          val wordsDataFrame = rdd.toDF()
//          
//          println("rdd : " + rdd.count())
//   
//          if (wordsDataFrame.count() > 0) {
//            
//            wordsDataFrame.createOrReplaceTempView(Config.SPARK_TMP_TABLE)
//            
//            spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//            
//            spark.sql("insert into yisadata.pass_info_10 Select * from tmp_pass_info DISTRIBUTE BY dateid")
//            
//          }
//          
//          // 手动删除RDD
//          rdd.unpersist()
//          
//        }
//      }
//      
//    streamingContext.start()
//    streamingContext.awaitTermination()
//
//  }  
//
//  def getTomDay(date : String)={
//    
//    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
//    var yesterday=dateFormat.format(new Date(dateFormat.parse(date).getTime + 1 * 24 * 60 * 60 * 1000))
//    
//    yesterday
//    
//  } 
//    
//  def getDateId(x : String): Int = {
//    val format1 = new SimpleDateFormat("yyyyMMddHHmmss")
//    val format2 = new SimpleDateFormat("yyyyMMdd")
//
//    try {
//      if (x == ""){
//        var d = format2.format(new Date()).toInt
//        return d
//      }
//      else {
//        var d = format2.format(format1.parse(x)).toInt
//        return d
//      }
//    } catch {
//      case e: Exception => println("cdr parse timestamp wrong")
//    }
//    0
//  }
//  
//  def getDate(x: String): Date = {
//
//    val format = new SimpleDateFormat("yyyyMMddHHmmss")
//
//    try {
//      if (x == "")
//        return null
//      else {
//        val d = format.parse(x);
//        return d
//      }
//    } catch {
//      case e: Exception => println("cdr parse timestamp wrong")
//    }
//    null
//
//  } 
//  
//  def getOldDay():Int={
//    
//    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
//    var cal:Calendar=Calendar.getInstance()
//    cal.add(Calendar.DATE,-30)
//    var yesterday=dateFormat.format(cal.getTime()).toInt
//    
//    yesterday
//    
//  }  
//
//  def getTimestamp(x: String): Long = {
//
//    val format = new SimpleDateFormat("yyyyMMddHHmmss")
//
//    try {
//      if (x == "")
//        return 0
//      else {
//        val d = format.parse(x);
//        val t = d.getTime()/1000
//        return t
//      }
//    } catch {
//      case e: Exception => println("cdr parse timestamp wrong")
//    }
//
//    0
//
//  }
//
//}
//
