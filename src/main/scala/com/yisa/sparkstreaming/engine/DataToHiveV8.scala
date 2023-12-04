package com.yisa.sparkstreaming.engine

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import com.google.gson.Gson
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.google.gson.reflect.TypeToken
import com.yisa.sparkstreaming.model.PassInfo2
import com.yisa.sparkstreaming.model.PassinfoForHive
import com.yisa.sparkstreaming.model.PassinfoForHive
import com.yisa.sparkstreaming.source.Config
import com.yisa.sparkstreaming.source.SparkSessionSingletonModel
import java.util.Calendar
import com.yisa.sparkstreaming.model.PassinfoForHive3
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.PosixParser
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.CommandLine

/**
 * 
 * 测试当前正在执行的DataToHive是否丢失数据
 *
 */
object DataToHive8 {

  def main(args: Array[String]) = {
    
    var cmd : CommandLine = null
    
    val options : Options = new Options()
    
    try{
      
      var zkServer : Option = new Option("zk_server", true, "输入zookeeper服务器地址")
      
      zkServer.setRequired(true)
      
      options.addOption(zkServer)
      
      var groupId : Option = new Option("group_id", true, "输入kafka group id")
      
      groupId.setRequired(true)
      
      options.addOption(groupId)
      
      val parser : PosixParser = new PosixParser()
      
      cmd = parser.parse(options, args)
      
    } catch {
      
      case ex : MissingOptionException => {
        
        println(ex)
        
        println("--zk_server <value> : " + options.getOption("zk_server").getDescription)
        
        System.exit(1)
      }
    }
    
    var zk_server_str = ""
    if(cmd != null && cmd.hasOption("zk_server")){
      zk_server_str = cmd.getOptionValue("zk_server")
    }
    
    Config.initConfig(zk_server_str)
    println("检验数据是否丢失")
    println(Config.showString())

    var Group_ID = ""
    if(cmd != null && cmd.hasOption("group_id")){
      Group_ID = cmd.getOptionValue("group_id")
    }

    var sprakConf = new SparkConf().setAppName("DataToHive8")
    var sc = new SparkContext(sprakConf)
    val streamingContext = new StreamingContext(sc, Minutes(Config.READ_KAFKA_DATA_TIME))

    //    val kafkaStream = KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 1), StorageLevel.MEMORY_AND_DISK_SER)

    val numDStreams = 10
    val kafkaStream = (1 to numDStreams).map { _ =>
      KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 5), StorageLevel.MEMORY_AND_DISK_SER)
    }

    val unionStream = streamingContext.union(kafkaStream)

    var data = unionStream.filter(!_._2.toString().equals(""))

    println("kafkaStream : " + unionStream.count())
    var aa = data.map(line_data => {

      var line = line_data._2
      var passInfo2: PassInfo2 = null
      
      try {
        val gson = new Gson
        val mapType = new TypeToken[PassInfo2] {}.getType
        passInfo2 = gson.fromJson[PassInfo2](line, mapType)
      } catch {
        case ex: Exception => {
          println("json数据接收异常 ：" + line)
        }
      }

      if (passInfo2 == null) {
        println("json数据接收异常 ：" + line)
      }

      var inputBean = PassinfoForHive3.apply(passInfo2.id,
        passInfo2.plateNumber,
        getTimestamp(passInfo2.captureTime + ""),
        -1,
        passInfo2.colorId,
        passInfo2.modelId,
        passInfo2.brandId,
        passInfo2.levelId,
        passInfo2.yearId,
        passInfo2.feature,
        passInfo2.locationuuid,
        passInfo2.lastCaptured,
        passInfo2.isSheltered,
        passInfo2.createTime,
        passInfo2.regionCode,
        passInfo2.directionId,
        getDateId(passInfo2.captureTime + ""))

      inputBean

    })

    aa.foreachRDD { rdd =>
      {
        val warehouseLocation = Config.SPARK_WARE_HOUSE_LOCATION
        val spark = SparkSessionSingletonModel.getInstance(warehouseLocation)

        import spark.implicits._

        // Convert RDD[String] to DataFrame
        val wordsDataFrame = rdd.toDF()

        println("rdd : " + rdd.count())

        if (wordsDataFrame.count() > 0) {

          wordsDataFrame.createOrReplaceTempView(Config.SPARK_TMP_TABLE)

          spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

          spark.sql("insert into yisadata.pass_info_10 Select * from tmp_pass_info DISTRIBUTE BY dateid")

        }

        // 手动删除RDD
        rdd.unpersist()

      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def getTomDay(date: String) = {

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
    var yesterday = dateFormat.format(new Date(dateFormat.parse(date).getTime + 1 * 24 * 60 * 60 * 1000))

    yesterday

  }

  def getDateId(x: String): Int = {
    val format1 = new SimpleDateFormat("yyyyMMddHHmmss")
    val format2 = new SimpleDateFormat("yyyyMMdd")

    try {
      if (x == "") {
        var d = format2.format(new Date()).toInt
        return d
      } else {
        var d = format2.format(format1.parse(x)).toInt
        return d
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    0
  }

  def getDate(x: String): Date = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        return d
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    null

  }

  def getOldDay(): Int = {

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -30)
    var yesterday = dateFormat.format(cal.getTime()).toInt

    yesterday

  }

  def getTimestamp(x: String): Long = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return 0
      else {
        val d = format.parse(x);
        val t = d.getTime() / 1000
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    0

  }

}

