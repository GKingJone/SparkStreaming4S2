package com.yisa.sparkstreaming.engine

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
import com.yisa.sparkstreaming.model.PassInfoForEs
import com.yisa.sparkstreaming.model.PassinfoForLast
import com.yisa.sparkstreaming.source.Config10

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
//import org.elasticsearch.spark.rdd.EsSpark
//import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import com.yisa.sparkstreaming.model.PassInfoForEsHbase
import com.yisa.sparkstreaming.model.PassInfoForEs
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
import com.yisa.sparkstreaming.model.PassInfoForEs

/**
 * 低级Api实现
 *
 * ES批量数据的写入实现
 *
 * 流程：入HBase 入ES 成功：更新zk
 */
object DataToESAndHbase6 {

  def main(args: Array[String]) {

    // hbase表名
    var hbaseTableNameForEs: String = "vehicleinfosForES"

    // hbase初次入城的表名
    var hbaseTableNameForFirstCity: String = "last_captured1"

    // ES的节点地址
    var esNodeIp = "128.127.120.243"

    // ES的数据库
    var esDataBaseName = "vehicle"
    
    var groupId = "gid2017032800004"
    
    // ES的数据库表
    var esDataBaseType = "info"
    
    var kafkaHistoryData = "history_passinfos"
    
    var zk_old = "gpu3,gpu5,gpu9"

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

    var sparkConf = new SparkConf().setAppName("DataToESAndHbase")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14047")

    // ES
    //    sparkConf.set("es.index.auto.create", "true")
    //    sparkConf.set(ConfigurationOptions.ES_NODES, esNodeIp) //节点信息  

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(30))

    //  producer
//    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
//      val kafkaProducerConfig = {
//        val p = new Properties()
//        p.setProperty("bootstrap.servers", Config10.KAFKA_BROKER_ID)
//        p.setProperty("key.serializer", classOf[StringSerializer].getName)
//        p.setProperty("value.serializer", classOf[StringSerializer].getName)
//        p
//      }
//      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
//    }
    //  producer

    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = Config10.KAFKA_TOPIC
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

          var captureTime = 0L
          var x = passInfo2.captureTime + ""
          try {
            if (x == "")
              captureTime = 0L
            else {
              var d = format.parse(x);
              var t = d.getTime() / 1000
              captureTime = t
            }
          } catch {
            case e: Exception => println("cdr parse timestamp wrong")
          }

          // 实例化
          var sBuilder = new StringBuilder();
          sBuilder.append(passInfo2.nodeId).append("_").append(passInfo2.locationId);
          var id = sBuilder.toString();
          var A001 = UUID.randomUUID().toString().replace("-", "")
          var A002 = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "")
          var A003 = passInfo2.levelId
          if (passInfo2.levelId == null) {
            A003 = -1
          }
          var A004 = passInfo2.yearId
          if (passInfo2.yearId == null) {
            A004 = -1
          }
          var A005 = passInfo2.modelId
          if (passInfo2.modelId == null) {
            A005 = -1
          }
          var A006 = passInfo2.brandId
          if (passInfo2.brandId == null) {
            A006 = -1
          }
          var A007 = passInfo2.plateNumber
          if (passInfo2.plateNumber == null) {
            A007 = ""
          }
          var A008 = passInfo2.regionCode
          if (passInfo2.regionCode == null) {
            A008 = -1
          }
          var A009 = captureTime
          var A011 = passInfo2.partType
          if (passInfo2.partType == null) {
            A011 = -1
          }
          var A012 = passInfo2.colorId
          if (passInfo2.colorId == null) {
            A012 = -1
          }
          var A013 = passInfo2.directionId
          if (passInfo2.directionId == null) {
            A013 = ""
          }
          var A014 = passInfo2.plateTypeId
          if (passInfo2.plateTypeId == null) {
            A014 = -1
          }
          var A015 = passInfo2.isSheltered
          if (passInfo2.isSheltered == null) {
            A015 = -1
          }
          var A016 = passInfo2.fakeplates
          if (passInfo2.fakeplates == null) {
            A016 = -1
          }
          var A017 = passInfo2.lastCaptured
          if (passInfo2.lastCaptured == null) {
            A017 = -1
          }
          var A018 = passInfo2.jiujia
          if (passInfo2.jiujia == null) {
            A018 = -1
          }
          var A019 = passInfo2.xidu
          if (passInfo2.xidu == null) {
            A019 = -1
          }
          var A020 = passInfo2.zuijia
          if (passInfo2.zuijia == null) {
            A020 = -1
          }
          var A021 = passInfo2.zaitao
          if (passInfo2.zaitao == null) {
            A021 = -1
          }
          var A022 = passInfo2.zhongdian
          if (passInfo2.zhongdian == null) {
            A022 = -1
          }
          var A023 = passInfo2.shean
          if (passInfo2.shean == null) {
            A023 = -1
          }
          var A024 = passInfo2.wuzheng
          if (passInfo2.wuzheng == null) {
            A024 = -1
          }
          var A025 = passInfo2.yuqi
          if (passInfo2.yuqi == null) {
            A025 = -1
          }
          var A026 = passInfo2.gaoweidiqu
          if (passInfo2.gaoweidiqu == null) {
            A026 = -1
          }
          var A027 = passInfo2.teshugaowei
          if (passInfo2.teshugaowei == null) {
            A027 = -1
          }
          var A040 = passInfo2.weizhang
          if (passInfo2.weizhang == null) {
            A040 = -1
          }
          var S002 = passInfo2.createTime
          if (passInfo2.createTime == null) {
            S002 = 0l
          }

          // Hbase
          var A033 = passInfo2.carBox
          if (passInfo2.carBox == null) {
            A033 = ""
          }
          var A034 = passInfo2.imageUrl
          if (passInfo2.imageUrl == null) {
            A034 = ""
          }
          var A036 = passInfo2.recPlateNumber
          if (passInfo2.recPlateNumber == null) {
            A036 = ""
          }
          var A037 = "" //passInfo2.vinCode
          var A038 = "" //passInfo2.realBrandId
          var A039 = passInfo2.locationId
          if (passInfo2.locationId == null) {
            A039 = ""
          }
          var A052 = passInfo2.feature
          if (passInfo2.feature == null) {
            A052 = ""
          }

          // 初次入城-start
          // lastCaptured = -1表示不满足初次入城的条件
          // 目的：求出正常情况的lastCaptured
//          var plateNumber = passInfo2.plateNumber // 车牌
//          var lastCaptured: Long = 0 // 初次入城时间
//          if (plateNumber != null && !"".equals(plateNumber) && !plateNumber.contains("无")
//            && !plateNumber.contains("未") && plateNumber.length() >= 6 && plateNumber.length() <= 10) {
//
//            // 查询Hbase
//            val hbaseConn = HBaseConnectManager.getConnet(zk_server_str)
//            val table = hbaseConn.getTable(TableName.valueOf(hbaseTableNameForFirstCity))
//            var get = new Get(Bytes.toBytes(passInfo2.plateNumber.reverse.toString()));
//
//            var result = table.get(get)
//
//            // 非第一次入城
//            if (result != null && result.size() > 0) {
//
//              var ltHbase = 0l
//
//              var cellList = result.listCells()
//              for (i <- 0 to (cellList.size() - 1)) {
//                var c = cellList.get(i)
//                var cpf = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength())
//
//                // 初次入城的时间
//                if (cpf != null && cpf.equals("ct")) {
//                  var cpHbase = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength).toString().toLong
//                  var cpKafka = captureTime
//                  if (cpHbase <= cpKafka) {
//                    // 接收实时数据的场合，全量更新ES和Hbase
//                    lastCaptured = cpKafka - cpHbase
//
//                  } else {
//
//                    lastCaptured = cpKafka - cpHbase
//
//                  }
//                }
//
//                if (cpf != null && cpf.equals("lt")) {
//                  ltHbase = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength).toString().toLong
//                }
//
//                if (((-lastCaptured) + ltHbase) < 12 * 60 && ltHbase != 0) {
//
//                  if (lastCaptured >= -1)
//                    lastCaptured = -1
//
//                }
//              }
//
//            } else {
//
//              // 第一次入城-Start
//              //如果captureTime大于当前时间，不更新到Hbase
//              if (captureTime == 0) {
//                lastCaptured = -1
//              }
//
//              lastCaptured = 0
//              // 第一次入城-END0
//
//            }
//
//          } else {
//            lastCaptured = -1
//          }

          
          // 初次入城-end

          var inputBean = PassInfoForEsHbase.apply(A001,
            A002,
            A003,
            A004,
            A005,
            A006,
            A007,
            A008,
            A009,
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
            A040,
            S002,
            A033,
            A034,
            A036,
            A037,
            A038,
            A039,
            A052)

          inputBean

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

            def convert(triple: (PassInfoForEsHbase)) = {
              var input = triple
              var rowkey = triple.A001
              var A002 = triple.A002
              var A003 = triple.A003
              var A004 = triple.A004
              var A005 = triple.A005
              var A006 = triple.A006
              var A007 = triple.A007
              var A008 = triple.A008
              var A009 = triple.A009
              var A011 = triple.A011
              var A012 = triple.A012
              var A013 = triple.A013
              var A014 = triple.A014
              var A015 = triple.A015
              var A016 = triple.A016
              var A017 = triple.A017
              var A018 = triple.A018
              var A019 = triple.A019
              var A020 = triple.A020
              var A021 = triple.A021
              var A022 = triple.A022
              var A023 = triple.A023
              var A024 = triple.A024
              var A025 = triple.A025
              var A026 = triple.A026
              var A027 = triple.A027
              var A040 = triple.A040
              var S002 = triple.S002

              var A033 = triple.A033
              var A034 = triple.A034
              var A036 = triple.A036
              var A037 = triple.A037
              var A038 = triple.A038
              var A039 = triple.A039
              var A052 = triple.A052

              var p = new Put(Bytes.toBytes(rowkey))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A002"), Bytes.toBytes(A002))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A003"), Bytes.toBytes(A003))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A004"), Bytes.toBytes(A004))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A005"), Bytes.toBytes(A005))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A006"), Bytes.toBytes(A006))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A007"), Bytes.toBytes(A007))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A008"), Bytes.toBytes(A008))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A009"), Bytes.toBytes(A009))
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
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A040"), Bytes.toBytes(A040))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("S002"), Bytes.toBytes(S002))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A033"), Bytes.toBytes(A033))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A034"), Bytes.toBytes(A034))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A036"), Bytes.toBytes(A036))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A037"), Bytes.toBytes(A037))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A038"), Bytes.toBytes(A038))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A039"), Bytes.toBytes(A039))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("A052"), Bytes.toBytes(A052))
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
            var aaEs = aa.map { esHbase =>

              var inputBean = PassInfoForEs.apply(esHbase.A001,
                esHbase.A002,
                esHbase.A003,
                esHbase.A004,
                esHbase.A005,
                esHbase.A006,
                esHbase.A007,
                esHbase.A008,
                esHbase.A009,
                esHbase.A011,
                esHbase.A012,
                esHbase.A013,
                esHbase.A014,
                esHbase.A015,
                esHbase.A016,
                esHbase.A017,
                esHbase.A018,
                esHbase.A019,
                esHbase.A020,
                esHbase.A021,
                esHbase.A022,
                esHbase.A023,
                esHbase.A024,
                esHbase.A025,
                esHbase.A026,
                esHbase.A027,
                esHbase.A040,
                esHbase.S002)

              inputBean
            }

            aaEs.foreachPartition { ap =>

              val client = ESClient.getInstance(esDataBaseName, esNodeIp)
              val bulkRequest: BulkRequestBuilder = client.prepareBulk()

              var count = 0
              ap.foreach { x =>

                var esId = x.A001

                var captureTime = x.A009

                var format = new SimpleDateFormat("yyyyMMddHHmmss")
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

          // 初次入城入Hbase-Start
          if (false) {

            var isAa = aa.filter { x => x.A017 >= 0 } //.coalesce(20)

            if (!isAa.isEmpty()) {
              // 新API
              sc.hadoopConfiguration.set("hbase.zookeeper.quorum", zk_server_str)
              sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
              sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableNameForFirstCity)

              val job = new Job(sc.hadoopConfiguration)
              job.setOutputKeyClass(classOf[ImmutableBytesWritable])
              job.setOutputValueClass(classOf[Result])
              job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

              // step 2: rdd mapping to table

              // 在 HBase 中表的 schema 一般是这样的
              // *row   cf:col_1    cf:col_2
              // 而在Spark中，我们操作的是RDD元组，yearid_platenumber_direction_colorid_modelid_brandid_levelid_lastcaptured_issheltered_solrid
              // 我们需要将 *RDD[(uid:Int, name:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
              // 我们定义了 convert 函数做这个转换工作
              def convert(triple: (PassInfoForEsHbase)) = {
                var input = triple
                var rowkey = triple.A007.reverse.toString()
                var id = triple.A001
                var ct = triple.A009.toString()
                var lt = triple.A017.toString()
                var p = new Put(Bytes.toBytes(rowkey))
                p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id))
                p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"), Bytes.toBytes(ct))
                p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lt"), Bytes.toBytes(lt))
                (new ImmutableBytesWritable, p)
              }

              var aaa = isAa.map(convert)

              aaa.saveAsNewAPIHadoopDataset(job.getConfiguration())

              print("初次入城的数据量 ： " + isAa.count() + "   ")

            } else {
              print("初次入城的数据量 ： " + 0 + "   ")
            }

          }
          // 初次入城人Hbase-End

          // 历史数据推送kafka-Start

//          if (false) {
//
//            var historyRdd = aa.filter { x => x.A017 < -60 }
//            historyRdd.foreachPartition(rdd => {
//              if (!rdd.isEmpty) {
//                rdd.foreach(record => {
//
//                  val gsonEs = new Gson
//                  var jsonHis = gsonEs.toJson(record)
//                  kafkaProducer.value.send(kafkaHistoryData, jsonHis)
//                })
//              }
//            })
//
//            print("  历史数据推送 :" + historyRdd.count() + "   ")
//          }
          // 历史数据推送kafka-End

          var formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          var nowDate = formatRdd.format(new Date())

          println(nowDate + " rdd的长度 : " + aa.count())
          // 初次入城入HBase-End

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