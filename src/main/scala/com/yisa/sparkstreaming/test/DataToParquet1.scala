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
//import org.apache.spark.sql.SaveMode
//import com.yisa.sparkstreaming.model.PassinfoForHive3
//
///**
//* @author liliwei
//* @date  2016年9月9日 
//* 
//*/
//object DataToParquet {
//
//  def main(args: Array[String]) = {
//    
//    Config.initConfig();
//    
//    var Group_ID = Config.KAFKA_GROUP_ID
//    
//    println("Group_ID : " + Group_ID)
//
//    var sprakConf = new SparkConf().setAppName(Config.APP_NAME)
//    var sc = new SparkContext(sprakConf)
//    val streamingContext = new StreamingContext(sc, Minutes(Config.READ_KAFKA_DATA_TIME))
//
//    val kafkaStream = KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 20), StorageLevel.MEMORY_AND_DISK_SER)
//
//    var data = kafkaStream.filter(!_._2.toString().equals(""))
//    
//    println("kafkaStream : " + kafkaStream.count())
//    
////    val igniteContext = new IgniteContext[Integer, Integer](sc, 
////    () => new IgniteConfiguration())
//
//    try {
//      var aa = data.map(line_data => {
//  
//        var line = line_data._2
//  
//        val gson = new Gson
//        val mapType = new TypeToken[PassInfo2] {}.getType
//        var passInfo2 = gson.fromJson[PassInfo2](line, mapType)
//        
//        var inputBean = PassinfoForHive3.apply(passInfo2.id,
//          passInfo2.plateNumber,
//          getTimestamp(passInfo2.captureTime + ""),
//  
//          passInfo2.directionId,
//          passInfo2.colorId,
//          passInfo2.modelId,
//          passInfo2.brandId,
//          passInfo2.levelId,
//          passInfo2.yearId,
//          passInfo2.feature,
//          passInfo2.locationuuid,
//          passInfo2.lastCaptured,
//          passInfo2.isSheltered,
//          passInfo2.createTime,
//          getDateId(passInfo2.captureTime + "")
//          )
//  
//        inputBean
//  
//      })
//  
//      aa.filter(_.dateid > getOldDay()).foreachRDD { rdd =>
//        {
//          
//          
//    
//          val formatDate = new SimpleDateFormat("yyyyMMdd")
//          var dateInit = formatDate.format((new Date().getTime + 1 * 24 * 60 * 60 * 1024)) + "01"
//      
//          val formatSs = new SimpleDateFormat("yyyyMMddHHmmss")
//          val formatHh = new SimpleDateFormat("yyyyMMddHH")
//          var dateCom = formatHh.format(new Date())
//          if(dateInit.equals(dateCom)){
//            
//            println("执行数据合并和历史数据删除-开始时间：" + formatSs.format(new Date()))
//            
//             Thread.sleep(60 * 1000);
//            
//            dateInit = getTomDay(dateCom)
//            
//            println("执行数据合并和历史数据删除-结束时间：" + formatSs.format(new Date()))
//          }
//      
//          val warehouseLocation = Config.SPARK_WARE_HOUSE_LOCATION
//          val spark = SparkSessionSingletonModel.getInstanceP(warehouseLocation)
//          
//          import spark.implicits._
//          
//          val wordsDataFrame = rdd.toDF()
//         
//          wordsDataFrame.write.mode(SaveMode.Append).partitionBy("dateid").parquet(warehouseLocation + "yisadata.db/pass_info_parquet")
//     
//          // 手动删除RDD
//          rdd.unpersist()
//          
//        }
//      }
//      
//     } catch {
//          case e: Exception => {
//            println("task wrong!!!")
//            println(e.getMessage)
//
//          }
//     }
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
