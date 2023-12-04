package com.yisa.sparkstreaming.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.sql.Timestamp
import com.yisa.sparkstreaming.model.PassInfo2
import com.yisa.sparkstreaming.model.PassinfoForHive
import com.yisa.sparkstreaming.source.Config
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
* @author liliwei
* @date  2016年9月20日 
* 
*/
object DataToHive2 {

  def main(args: Array[String]) = {

    //	  var sprakConf = new SparkConf().setAppName("test").setMaster("spark://cdh1:7077").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    //    var sprakConf = new SparkConf().setAppName("test").setMaster("local").set("spark.sql.warehouse.dir", "file:///D:/spark-warehouse")
    val Group_ID = args(0)

    var sprakConf = new SparkConf().setAppName(Config.APP_NAME)
    var sc = new SparkContext(sprakConf)
//    val streamingContext = new StreamingContext(sc, Minutes(60))
      val streamingContext = new StreamingContext(sc, Seconds(10))

    val kafkaStream = KafkaUtils.createStream(streamingContext, Config.ZK_DATA_DIR, Group_ID, Map[String, Int](Config.KAFKA_TOPIC -> 1), StorageLevel.MEMORY_ONLY_SER)

    //过滤空值
    var dataFilter = kafkaStream.filter(!_._2.toString().equals(""))
    //key locationID  value PassinfoForHive
    var DataWithLocationID = dataFilter.map(line_data => {

      var line = line_data._2
//      println("line:" + line)

      val gson = new Gson
      val mapType = new TypeToken[PassInfo2] {}.getType
      var passInfo2 = gson.fromJson[PassInfo2](line, mapType)
      var passinfoForHive = PassinfoForHive.apply(passInfo2.id,
        passInfo2.plateNumber,
        getTimestamp(passInfo2.captureTime + ""),

        passInfo2.directionId,
        passInfo2.colorId,
        passInfo2.modelId,
        passInfo2.brandId,
        passInfo2.levelId,
        passInfo2.yearId,
        passInfo2.feature,
        getDateId(),
        passInfo2.locationId)

      (passInfo2.locationId, passinfoForHive)

    })

    DataWithLocationID.groupByKey().map(data => {

      val warehouseLocation = Config.SPARK_WARE_HOUSE_LOCATION
      val sparkSession = SparkSession
        .builder
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .config("spark.sql.shuffle.partitions", "1")
        .enableHiveSupport()
        .getOrCreate()
      import sparkSession.implicits._

      val passinfoIterable = data._2

      val passinfoDataFrame = passinfoIterable.toSeq.toDF()

      if (passinfoDataFrame.count() > 0) {
        // Create a temporary view
        passinfoDataFrame.createOrReplaceTempView(Config.SPARK_TMP_TABLE)
        passinfoDataFrame.printSchema()
//        passinfoDataFrame.show()
        //启用GZIP压缩
        sparkSession.sql("set hive.exec.compress.output=true")
        sparkSession.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
        //设置自动动态分区
        sparkSession.sql("set  hive.exec.dynamic.partition.mode=nonstrict")
        sparkSession.sql( "insert into yisadata.pass_info Select * from tmp_pass_info DISTRIBUTE BY locationid")
      }

      sparkSession.stop()

    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def getDateId(): Int = {
    val format = new SimpleDateFormat("yyyyMMdd")
    var today = new Date();
    format.format(today).toInt
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

  def getTimestamp(x: String): java.sql.Timestamp = {

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }

    null

  }

}

