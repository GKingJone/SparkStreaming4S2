package com.yisa.sparkstreaming.engine.ni

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.yisa.sparkstreaming.manager.KafkaManager
import com.yisa.sparkstreaming.model.PassInfo2
import com.yisa.sparkstreaming.model.PassinfoForLast
import com.yisa.sparkstreaming.source.Config10

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
//import org.elasticsearch.spark.rdd.EsSpark
//import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import com.yisa.sparkstreaming.model.PassInfoForEsHbase
import com.yisa.sparkstreaming.source.ESClient
import org.elasticsearch.action.bulk.BulkRequestBuilder
import com.yisa.sparkstreaming.source.HBaseConnectManager
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.TableName
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.network.InetAddresses
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.transport.netty4.Netty4Utils
import org.elasticsearch.action.ActionRequestValidationException
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.{ Failure, Success }
import org.apache.spark.broadcast.Broadcast
import org.apache.hadoop.hdfs.DFSClient.Conf
import org.apache.kafka.common.serialization.StringSerializer
import com.yisa.sparkstreaming.manager.KafkaSink
import com.yisa.sparkstreaming.model.PassInfoForKafkaNewInterface
import com.yisa.sparkstreaming.model.PassInfoForEsNewInterface

/**
 * 低级Api实现
 *
 * ES批量数据的写入实现
 *
 * 流程：入HBase 入ES 成功：更新zk
 */
object DataToESAndHbaseNewInterface {

  def main(args: Array[String]) {

    // hbase表名
    var hbaseTableNameForEs: String = "vehicleinfosForES"

    // ES的节点地址
    var esNodeIp = "172.22.0.43"

    // ES的数据库
    var esDataBaseName = "vehicle"

    var groupId = "gid20170050300001"

    // ES的数据库表
    var esDataBaseType = "info"

    var zk_old = "com1,com2,com3,com4,com5"

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

    var sparkConf = new SparkConf().setAppName("DataToESAndHbaseNewInterface")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14047")

    var sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    var ssc = new StreamingContext(sc, Seconds(30))

    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = "pre_analysis_passinfos" //Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest")

    // largest/smallest

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        var aa = rdd.coalesce(50, true).map(line_data => {

          var line = line_data._2

          var passInfo2: PassInfoForKafkaNewInterface = null

          try {
            val gson = new Gson
            val mapType = new TypeToken[PassInfoForKafkaNewInterface] {}.getType
            passInfo2 = gson.fromJson[PassInfoForKafkaNewInterface](line, mapType)
          } catch {
            case ex: Exception => {
              println("json数据接收异常 ：" + line)
            }
          }

          if (passInfo2 == null) {
            println("json数据接收异常 ：" + line)
          }

          passInfo2
        })

        // 先处理消息
        if (!aa.isEmpty()) {

          // 入HBase-Start-新API
          if (true) {
            sc.hadoopConfiguration.set("hbase.zookeeper.quorum", zk_old)
            sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
            sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableNameForEs)

            val job = new Job(sc.hadoopConfiguration)
            job.setOutputKeyClass(classOf[ImmutableBytesWritable])
            job.setOutputValueClass(classOf[Result])
            job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

            def convert(triple: (PassInfoForKafkaNewInterface)) = {
              var passInfo2 = triple
              // 实例化
              var rowkey = passInfo2.id
              var A002 = passInfo2.locationuuid
              var A003 = passInfo2.levelId
              var A004 = passInfo2.yearId
              var A005 = passInfo2.modelId
              var A006 = passInfo2.brandId
              var A007 = passInfo2.plateNumber
              var A008 = passInfo2.regionCode
              var A009 = passInfo2.captureTime
              var A010 = passInfo2.nodeId
              var A011 = passInfo2.partType
              var A012 = passInfo2.colorId
              var A013 = passInfo2.directionId
              var A014 = passInfo2.plateTypeId
              var A015 = passInfo2.plateCategoryId
              var A016 = passInfo2.lastCaptured
              var A017 = passInfo2.skylight
              var A018 = passInfo2.baggageHold
              var A019 = passInfo2.sprayWord
              var A020 = passInfo2.inspectionTag
              var A021 = passInfo2.sunShadeLeft
              var A022 = passInfo2.sunShadeRight
              var A023 = passInfo2.pendant
              var A024 = passInfo2.tissueBox
              var A025 = passInfo2.decoration
              var A026 = passInfo2.card
              var A027 = passInfo2.personLeft
              var A028 = passInfo2.personRight
              var A029 = passInfo2.backBurnerTail
              var A030 = passInfo2.sendTime
              var A031 = passInfo2.recPlateNumber
              var A032 = passInfo2.locationId
              var A033 = passInfo2.carBox
              var A034 = passInfo2.imageUrl
              var A035 = passInfo2.speed
              var A036 = passInfo2.feature

              var p = new Put(Bytes.toBytes(rowkey))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A002"), Bytes.toBytes(A002))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A003"), Bytes.toBytes(A003))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A004"), Bytes.toBytes(A004))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A005"), Bytes.toBytes(A005))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A006"), Bytes.toBytes(A006))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A007"), Bytes.toBytes(A007))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A008"), Bytes.toBytes(A008))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A009"), Bytes.toBytes(A009))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A010"), Bytes.toBytes(A010))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A011"), Bytes.toBytes(A011))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A012"), Bytes.toBytes(A012))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A013"), Bytes.toBytes(A013))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A014"), Bytes.toBytes(A014))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A015"), Bytes.toBytes(A015))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A016"), Bytes.toBytes(A016))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A017"), Bytes.toBytes(A017))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A018"), Bytes.toBytes(A018))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A019"), Bytes.toBytes(A019))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A020"), Bytes.toBytes(A020))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A021"), Bytes.toBytes(A021))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A022"), Bytes.toBytes(A022))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A023"), Bytes.toBytes(A023))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A024"), Bytes.toBytes(A024))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A025"), Bytes.toBytes(A025))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A026"), Bytes.toBytes(A026))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A027"), Bytes.toBytes(A027))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A028"), Bytes.toBytes(A028))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A029"), Bytes.toBytes(A029))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A030"), Bytes.toBytes(A030))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A031"), Bytes.toBytes(A031))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A032"), Bytes.toBytes(A032))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A033"), Bytes.toBytes(A033))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A034"), Bytes.toBytes(A034))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A035"), Bytes.toBytes(A035))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A036"), Bytes.toBytes(A036))
              (new ImmutableBytesWritable, p)
            }
            //
            var aaHbase = aa.map(convert)
            //
            aaHbase.saveAsNewAPIHadoopDataset(job.getConfiguration())

          }
          // 入HBase-End-新API

          // 入ES-Start
          if (true) {
            var aaEs = aa.map { passInfo2 =>

              var A001 = passInfo2.id
              var A002 = passInfo2.locationuuid
              var A003 = passInfo2.levelId
              var A004 = passInfo2.yearId
              var A005 = passInfo2.modelId
              var A006 = passInfo2.brandId
              var A007 = passInfo2.plateNumber
              var A008 = passInfo2.regionCode
              var A009 = passInfo2.captureTime
              var A010 = passInfo2.nodeId
              var A011 = passInfo2.partType
              var A012 = passInfo2.colorId
              var A013 = passInfo2.directionId
              var A014 = passInfo2.plateTypeId
              var A015 = passInfo2.plateCategoryId
              var A016 = passInfo2.lastCaptured
              var A017 = passInfo2.skylight
              var A018 = passInfo2.baggageHold
              var A019 = passInfo2.sprayWord
              var A020 = passInfo2.inspectionTag
              var A021 = passInfo2.sunShadeLeft
              var A022 = passInfo2.sunShadeRight
              var A023 = passInfo2.pendant
              var A024 = passInfo2.tissueBox
              var A025 = passInfo2.decoration
              var A026 = passInfo2.card
              var A027 = passInfo2.personLeft
              var A028 = passInfo2.personRight
              var A029 = passInfo2.backBurnerTail
              var A030 = passInfo2.sendTime
              var A031 = passInfo2.recPlateNumber
              var A032 = passInfo2.locationId
              
              var inputBean = PassInfoForEsNewInterface.apply(A001,
                A002,
                A003,
                A004,
                A005,
                A006,
                A007,
                A008,
                A009,
                A010,
                A011,
                A012,
                A013,
                A014,
                A015,
                A016,
                A017,
                A018,
                A019,
                A020,
                A021,
                A022,
                A023,
                A024,
                A025,
                A026,
                A027,
                A028,
                A029,
                A030,
                A031,
                A032)

              inputBean
            }

            aaEs.foreachPartition { ap =>

              val client = ESClient.getInstance(esDataBaseName, esNodeIp)
              val bulkRequest: BulkRequestBuilder = client.prepareBulk()

              var count = 0
              ap.foreach { x =>

                var esId = x.A001

                var captureTime = x.A009

                var format2 = new SimpleDateFormat("yyyyMMdd")

                var typeES = ""
                try {
                  if (captureTime > 0) {
                    typeES = format2.format(captureTime * 1000).toString().substring(0, 6)
                  } else {
                    typeES = format2.format(new Date()).toString().substring(0, 6)
                  }
                } catch {
                  case e: Exception =>
                    println(e.getMessage + "  日期格式转换错误 ：captureTime=" + captureTime + "  typeES : " + typeES)
                    typeES = format2.format(new Date()).toString().substring(0, 6)
                }

                val gsonEs = new Gson

                if (x != null) {
                  try {
                    bulkRequest.add(client.prepareIndex(esDataBaseName + typeES, esDataBaseType, esId).setSource(gsonEs.toJson(x)))
                  } catch {
                    case ex: ActionRequestValidationException => {
                      println(ex.getMessage + "异常！" + esDataBaseName + typeES + "\t" + typeES + "\t" + esId + "\n" + gsonEs.toJson(x));
                    }
                  }

                  count = count + 1
                } else {
                  println("数据异常！")
                }

                if (count % 20000 == 0) {
                  bulkRequest.execute().actionGet()
                  println("es提交20000条数据！")
                  count = 0
                }

              }

              if (count > 0)
                bulkRequest.execute().actionGet();

            }

          }
          // 入ES-END

          var formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          var nowDate = formatRdd.format(new Date())

          println(nowDate + " rdd的长度 : " + aa.count())

          // 再更新offsets
          km.updateZKOffsets(rdd)
        }

      } else {

        val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        var nowDate = formatRdd.format(new Date())

        println(nowDate + " 失败rdd的长度 ：" + 0)

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