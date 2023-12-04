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
import com.yisa.sparkstreaming.source.RedisClient
import com.yisa.sparkstreaming.source.RedisClusterClient
import com.yisa.sparkstreaming.source.RedisClusterClientZK
import com.yisa.sparkstreaming.source.HBaseConnectManager
import org.apache.hadoop.hbase.TableName
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Put
import java.util.UUID
import org.apache.hadoop.hbase.util.Bytes
import java.sql.Timestamp
import scala.util.Random
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.Vector
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.BufferedMutator
import java.util.Collections

/**
 * 低级Api实现
 */
object DataToIndex {

  def processRdd(rdd: RDD[PassinfoForHive3], zkHostport: String): Unit = {
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

      //      wordsDataFrame.repartition(1).write.partitionBy("dateid","brandid").mode(SaveMode.Append).parquet(Config10.configs.get("alluxio_Path")+"/pass_info_index")

      val inittable_pass_info2 = wordsDataFrame.select("dateid", //0
        "recfeature", //1
        "yearid", //2
        "platenumber", //3
        "solrid", //4
        "capturetime", //5
        "direction", //6
        "colorid", //7
        "modelid", //8
        "brandid", //9
        "levelid", //10
        "locationid", //11
        "lastcaptured", //12
        "issheltered") //13

      //        inittable_pass_info2.collect().foreach(f)

      inittable_pass_info2.foreachPartition { row =>

        val jedis = RedisClusterClientZK.getJedisCluster(zkHostport)
        val hbaseConn = HBaseConnectManager.getConnet(zkHostport)
        val hbaseConfig = HBaseConnectManager.getConfig(zkHostport)

        val table_pass_yearid_index = hbaseConn.getTable(TableName.valueOf("pass_yearid_index"));
        val table_pass_info_index2 = hbaseConn.getTable(TableName.valueOf("pass_info_index2"));

        //        val table_pass_yearid_index = new HTable(hbaseConfig, TableName.valueOf("pass_yearid_index"))
        //        val table_pass_info_index2 = new HTable(hbaseConfig,TableName.valueOf("pass_info_index2"));

        //        table_pass_yearid_index.setAutoFlush(false, false)
        //        table_pass_info_index2.setAutoFlush(false, false)

        //       table_pass_yearid_index.setWriteBufferSize(10*1024*1024)

        //        table_pass_yearid_index.setWriteBufferSize(10*1024*1024)

        //        val puts = new Vector[Put]()
        //        val puts2 = new Vector[Put]()
        //        var puts = Collections.synchronizedList(new ArrayList[Put]());
        //        var puts2 = Collections.synchronizedList(new ArrayList[Put]());

        row.foreach { r =>
          {

            //-------------start insert hbase---------------------

            val put2_pass_info_index2 = new Put(Bytes.toBytes(r.getString(11) + "_" + r.getLong(5).toString()))

            var yearid = r.getInt(2).toString();
            if (yearid.length() == 1) {
              yearid = "000" + yearid
            } else if (yearid.length() == 2) {
              yearid = "00" + yearid
            } else if (yearid.length() == 3) {
              yearid = "0" + yearid
            }

            val random = new Random();
            var num = random.nextInt(999)
            var numS = num.toString();
            if (numS.length() == 1) {
              numS = "00" + numS
            } else if (numS.length() == 2) {
              numS = "0" + numS
            }

            val version = (r.getLong(5).toString() + yearid + numS).toLong

            //                val inittable_pass_info2 = wordsDataFrame.select("dateid", //0
            //        "recfeature", //1
            //        "yearid", //2
            //        "platenumber", //3
            //        "solrid", //4
            //        "capturetime", //5
            //        "direction", //6
            //        "colorid", //7
            //        "modelid", //8
            //        "brandid", //9
            //        "levelid", //10
            //        "locationid", //11
            //        "lastcaptured", //12
            //        "issheltered") //13
            //yearid_platenumber_direction_colorid_modelid_brandid_levelid_lastcaptured_issheltered_solrid
            val value_ = r.getInt(2) + "_" +
              r.getString(3) + "_" +
              r.getString(6) + "_" +
              r.getInt(7) + "_" +
              r.getInt(8) + "_" +
              r.getInt(9) + "_" +
              r.getInt(10) + "_" +
              r.getInt(12) + "_" +
              r.getInt(13) + "_" +
              r.getString(4)

            put2_pass_info_index2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pass"), version, Bytes.toBytes(value_))

            //            puts2.add(put2_pass_info_index2)
            table_pass_info_index2.put(put2_pass_info_index2)
            //-------------------end insert table_pass_info_index2 habse---------------------

            if (r.getString(1) != null && !r.getString(1).equals("")) {

              //-------------start insert hbase---------------------
              val date = new Date((r.getLong(5).toString() + "000").toLong)
              val format = new SimpleDateFormat("yyyyMMddHH")
              val timeid = format.format(date)

              val sBuilder = new StringBuilder();
              sBuilder.append(r.getInt(2)).append("_").append(timeid);
              val id = sBuilder.toString();
              val rowkey_uuid = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "");
              val put_pass_yearid_index = new Put(Bytes.toBytes(rowkey_uuid))

              //
              val byteData = Base64.getDecoder.decode(r.getString(1))
              var numRec: Long = 0
              for (i <- 0 until byteData.length) {
                var n = (byteData(i) & 0xff)
                numRec += n
              }
              var numRec2 = numRec % 1000
              var numRec2S = numRec2.toString()
              if (numRec2S.length() == 1) {
                numRec2S = "00" + numRec2
              } else if (numRec2S.length() == 2) {
                numRec2S = "0" + numRec2
              }

              //              
              put_pass_yearid_index.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yearids"), (r.getLong(5).toString() + numS + numRec2S).toLong, Bytes.toBytes(r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4)+ "_"+r.getLong(5)))
              //              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("capturetime"), (r.getLong(5).toString() + "000").toLong, Bytes.toBytes(r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4)))
              //        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yearids"), timestapVersion, Bytes.toBytes(r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4)))
              //              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("capturetime"), timestapVersion, Bytes.toBytes(r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4)))

              //              puts.add(put_pass_yearid_index)
              table_pass_yearid_index.put(put_pass_yearid_index)
              //-------------------end insert habse---------------------
 
              //-------------------start insert reids----------
              try {
                jedis.lpush(r.getInt(0) + "_" + r.getInt(2), r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4) + "_" + r.getLong(5))
              } catch {
                case ex: Exception => {
                  ex.printStackTrace()
                  println(r.getInt(0) + "_" + r.getInt(2) + "||" + r.getString(1) + "_" + r.getString(3) + "_" + r.getString(4) + "_" + r.getLong(5))
                } 
              }
              //-------------------end insert reids----------
            }

          }

        }
        //        table_pass_yearid_index.put(puts)
        //        table_pass_info_index2.put(puts2)

        table_pass_yearid_index.close()
        table_pass_info_index2.close()
      }
    }

    // 手动删除RDD
    rdd.unpersist()
  }

  def main(args: Array[String]) {

    var cmd: CommandLine = null

    val options: Options = new Options()

    try {

      var zkServer: Option = new Option("zk_server", true, "输入zookeeper服务器地址")

      zkServer.setRequired(true)

      options.addOption(zkServer)

      val parser: PosixParser = new PosixParser()

      cmd = parser.parse(options, args)

    } catch {

      case ex: MissingOptionException => {

        println(ex)

        println("--zk_server <value> : " + options.getOption("zk_server").getDescription)

        System.exit(1)
      }
    }

    var zk_server_str = ""
    if (cmd != null && cmd.hasOption("zk_server")) {
      zk_server_str = cmd.getOptionValue("zk_server")
    }

    Config10.initConfig(zk_server_str)

    var sparkConf = new SparkConf().setAppName("DataToIndex")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", Config10.SPARK_UI_PORT)

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(5))

    println("低级API实现")
    println(Config10.showIndexString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> Config10.configs.get("KAFKA_GROUP_ID_INDEX"),
      "auto.offset.reset" -> "largest")

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

          var format = new SimpleDateFormat("yyyyMMddHHmmss")
          var format2 = new SimpleDateFormat("yyyyMMdd")
          var captureTime_Timestamp = 0L
          var createTime_Timestamp = 0L
          var dateid = 0
          try {

            val captureTime_Date = format.parse(passInfo2.captureTime.toString());
            val createTime_Date = format.parse(passInfo2.createTime.toString());

            captureTime_Timestamp = captureTime_Date.getTime() / 1000
            createTime_Timestamp = createTime_Date.getTime() / 1000

            dateid = format2.format(captureTime_Date).toInt

          } catch {
            case e: Exception =>
              e.printStackTrace()
              println("cdr parse timestamp wrong")
          }

          var inputBean = PassinfoForHive3.apply(passInfo2.id,
            passInfo2.plateNumber,
            captureTime_Timestamp,
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
            createTime_Timestamp,
            passInfo2.regionCode,
            passInfo2.directionId,
            dateid)

          inputBean

        })

        // 先处理消息
        processRdd(aa, zk_server_str)

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
}