package com.yisa.sparkstreaming.engine

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.sparkstreaming.manager.KafkaManager
import com.yisa.sparkstreaming.model.PassInfo2
import com.yisa.sparkstreaming.model.PassinfoForHive3
import com.yisa.sparkstreaming.source.Config10
import com.yisa.sparkstreaming.source.SparkSessionSingletonModel

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.Seconds

/**
 * 低级Api实现
 */
object DataToParquet {

  def processRdd(rdd: RDD[PassinfoForHive3]): Unit = {
    val warehouseLocation = Config10.SPARK_WARE_HOUSE_LOCATION
    val spark = SparkSessionSingletonModel.getInstance(warehouseLocation)

    val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var nowDate = formatRdd.format(new Date())

    import spark.implicits._

    // Convert RDD[String] to DataFrame
    
    
    
    
    val wordsDataFrame = rdd.toDF()

    println(nowDate + " rdd的长度 : " + rdd.count())

    if (wordsDataFrame.count() > 0) {
      
      
//        wordsDataFrame.createOrReplaceTempView(Config10.SPARK_TMP_TABLE)
//
//      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      
      wordsDataFrame.repartition(1).write.partitionBy("dateid","brandid").mode(SaveMode.Append).parquet(Config10.configs.get("alluxio_Path")+"/pass_info_index")

    }

    // 手动删除RDD
    rdd.unpersist()
  }

  def main(args: Array[String]) {
    
    var cmd : CommandLine = null
    
    val options : Options = new Options()
    
    try{
      
      var zkServer : Option = new Option("zk_server", true, "输入zookeeper服务器地址")
      
      zkServer.setRequired(true)
      
      options.addOption(zkServer)
      
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
    
    Config10.initConfig(zk_server_str)

    var sparkConf = new SparkConf().setAppName("DataToIndex")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", Config10.SPARK_UI_PORT)

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(Config10.READ_KAFKA_DATA_TIME))
    
    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> "gid201703021003001",  
      "auto.offset.reset" -> "largest" )

    // largest/smallest

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        var aa = rdd.map(line_data => {

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
            getTimestamp(passInfo2.createTime + ""),
            passInfo2.regionCode,
            passInfo2.directionId,
            getDateId(passInfo2.captureTime + ""))

          inputBean

        })

        // 先处理消息
        processRdd(aa)

        // 再更新offsets
        km.updateZKOffsets(rdd)

      } else {

          val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      
          var nowDate = formatRdd.format(new Date())
          
          println(nowDate + " rdd的长度 ：" + 0)
        
      }
    })

    ssc.start()
    ssc.awaitTermination()
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
}