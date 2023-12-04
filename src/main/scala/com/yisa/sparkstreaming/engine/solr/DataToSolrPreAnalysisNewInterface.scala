package com.yisa.sparkstreaming.engine.solr

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

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
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
import com.yisa.sparkstreaming.source.RedisConnectionPool
import redis.clients.jedis.Jedis
import com.yisa.sparkstreaming.model.PassInfoForKafkaNewInterface
import java.util.ArrayList
import java.util.Collections
import java.sql.DriverManager
import com.yisa.sparkstreaming.model.PassinfoForLast
import com.yisa.sparkstreaming.source.ConfigIndex

/**
 * 低级Api实现
 *
 * 预处理流程
 *
 * lastCaptured 初次入城
 * id DL ID,rowkey
 * locationuuid 卡口编号
 * brandId,levelId,plateId,modelId
 * regionCode
 * plateTypeId
 */
object DataToSolrPreAnalysisNewInterface {

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

    val ZK_TYPE = "spark_index"

    ConfigIndex.initConfig(zk_server_str,ZK_TYPE)

    // hbase初次入城的表名
    var hbaseTableNameForFirstCity = ConfigIndex.preHbaseTableNameForFirstCity //"last_captured1"

    var groupId = ConfigIndex.preGroupId //"gid2017050800001"

    var kafkaPreData = ConfigIndex.preKafkaProducerTopicName //"presolr_analysis_passinfos"

    var batchTime = ConfigIndex.preReadKafkaBatchTime //10

    var preUiPort = ConfigIndex.preSparkUI //"14049"

    var preTopic = ConfigIndex.preKafkaConsumerTopicName //"recognitionMessage"

    var kafkaBrokerID = ConfigIndex.comKafkaBrokerIDs //Config10.KAFKA_BROKER_ID

    var preConcurrentSize = ConfigIndex.preConcurrentSize //100

    var URL = ConfigIndex.jdbcGBUrl //"jdbc:mysql://128.127.120.200:3306/gb_v12?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull"
    var USERNAME = ConfigIndex.jdbcGBUsername //"root"
    var PASSWORD = ConfigIndex.jdbcGBPassword //"root"

    var sparkConf = new SparkConf().setAppName("DataToSolrPreAnalysisNewInterface")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", preUiPort)

    var sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    var ssc = new StreamingContext(sc, Seconds(batchTime))

    //  producer
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", kafkaBrokerID)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    //  producer

    println("低级API实现")
    println(ConfigIndex.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    // 初期化Mysql
    var initMap: Map[String, Integer] = Map()

    try {
      val DRIVER = "com.mysql.jdbc.Driver";

      Class.forName(DRIVER);
      var conn = DriverManager.getConnection(URL, USERNAME, PASSWORD)

      val sql = "select yearID,brandID,modelID  from  car_year";
      val sql1 = "select node_id,loc_id,region_code from mon_location"
      val sql2 = "select plate_typeid, plate_colorid from car_plate_type"

      val pstmt = conn.prepareStatement(sql)
      var rs = pstmt.executeQuery();

      while (rs.next()) {

        var yearID = rs.getLong("yearID")
        var brandID = rs.getInt("brandID");
        var modelID = rs.getInt("modelID");

        initMap += ("yb_" + yearID.toString() -> brandID)
        initMap += ("ym_" + yearID.toString() -> modelID)
      }

      val pstmt1 = conn.prepareStatement(sql1)
      var rs1 = pstmt1.executeQuery();

      while (rs1.next()) {

        var nodeID = rs1.getString("node_id")
        var locID = rs1.getString("loc_id");
        var regionCode = rs1.getInt("region_code");

        initMap += ("nlr_" + nodeID + "_" + locID -> regionCode)
      }

      val pstmt2 = conn.prepareStatement(sql2)
      var rs2 = pstmt2.executeQuery();

      while (rs2.next()) {

        var plateTypeID = rs2.getInt("plate_typeid")
        var plateColorID = rs2.getInt("plate_colorid");

        initMap += ("pp_" + plateTypeID.toString() -> plateColorID)
      }

      rs.close()
      pstmt.close()
      rs1.close()
      pstmt1.close()
      rs2.close()
      pstmt2.close()
      conn.close()
    } catch {
      case e: Exception => {
        println(e.getMessage + "Mysql获取gb连接失败！请检查表car_year,mon_location,car_plate_type是否存在！")
        sc.stop()
      }
    }

    println("initMap : " + initMap.toString())
    var initMapMemory = sc.broadcast(initMap)
    // 初期化mysql

    val topics = preTopic
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokerID,
      "group.id" -> groupId,
      "auto.offset.reset" -> "largest")

    // largest/smallest

    val km = new KafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        var aa = rdd.coalesce(preConcurrentSize, true).map(line_data => {

          var line = line_data._2

          var passInfo2: PassInfoForKafkaNewInterface = null

          var esId: String = "" // ES ID Hbase Rowkey
          var locationuuid: String = "" // 卡口
          var modelId: Int = -1 // 车辆型号
          var brandId: Int = -1 // 车辆品牌
          var regionCode: Int = 0 // 行政区划编码
          var plateTypeId: Int = -1 // 车牌类型
          var lastCaptured: Long = 0 // 初次入城时间

          try {
            var gson = new Gson
            var mapType = new TypeToken[PassInfoForKafkaNewInterface] {}.getType
            passInfo2 = gson.fromJson[PassInfoForKafkaNewInterface](line, mapType)
          } catch {
            case ex: Exception => {
              println(ex.getMessage + "  json数据接收异常 ：" + line)
            }
          }
          if (passInfo2 == null) {
            println("  json数据接收异常 ：" + line)
          }

          // 实例化
          esId = UUID.randomUUID().toString().replace("-", "")

          var sBuilder = new StringBuilder();
          if (passInfo2.nodeId == null) {
            passInfo2.nodeId = ""
          }
          if (passInfo2.locationId == null) {
            passInfo2.locationId = ""
          }
          sBuilder.append(passInfo2.nodeId).append("_").append(passInfo2.locationId);
          var id = sBuilder.toString();
          locationuuid = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "")

          passInfo2.id = esId
          passInfo2.locationuuid = locationuuid

          // 车辆型号,车辆品牌
          var yearId = passInfo2.yearId
          if (yearId != 0) {
            if (initMapMemory.value.contains("yb_" + yearId.toString())) {
              passInfo2.brandId = initMapMemory.value("yb_" + yearId.toString())
            } else {
              passInfo2.brandId = -1
            }
            if (initMapMemory.value.contains("ym_" + yearId.toString())) {
              passInfo2.modelId = initMapMemory.value("ym_" + yearId.toString())
            } else {
              passInfo2.modelId = -1
            }
          } else {
            passInfo2.brandId = -1
            passInfo2.modelId = -1
          }

          // 行政区划编码
          var nodeId = passInfo2.nodeId
          var loc_id = passInfo2.locationId
          var regionKey = "nlr_" + nodeId + "_" + loc_id
          if (initMapMemory.value.contains(regionKey)) {
            passInfo2.regionCode = initMapMemory.value(regionKey)
          } else {
            passInfo2.regionCode = 0
          }

          // 车牌类型
          var plateCategoryId = passInfo2.plateCategoryId
          if (initMapMemory.value.contains("pp_" + plateCategoryId.toString())) {
            passInfo2.plateTypeId = initMapMemory.value("pp_" + plateCategoryId.toString())
          } else {
            passInfo2.plateTypeId = -1
          }

          if (true) {
            // 初次入城-start
            // lastCaptured = -1表示不满足初次入城的条件
            // 目的：求出正常情况的lastCaptured
            var captureTime = passInfo2.captureTime
            var plateNumber = passInfo2.plateNumber // 车牌
            var nowDate = System.currentTimeMillis() / 1000l
            if (plateNumber != null && !"".equals(plateNumber) && !plateNumber.contains("无")
              && !plateNumber.contains("未") && plateNumber.length() >= 6 && plateNumber.length() <= 10
              && captureTime < (nowDate + 24 * 60 * 60)) {

              // 查询Hbase
              val hbaseConn = HBaseConnectManager.getConnet(zk_server_str)
              val table = hbaseConn.getTable(TableName.valueOf(hbaseTableNameForFirstCity))
              var get = new Get(Bytes.toBytes(passInfo2.plateNumber.reverse.toString()));

              var result = table.get(get)

              // 非第一次入城
              if (result != null && result.size() > 0) {

                var ltHbase = 0l

                var cellList = result.listCells()
                for (i <- 0 to (cellList.size() - 1)) {
                  var c = cellList.get(i)
                  var cpf = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength())

                  // 初次入城的时间
                  if (cpf != null && cpf.equals("ct")) {
                    var cpHbase = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength).toString().toLong
                    var cpKafka = captureTime
                    if (cpHbase <= cpKafka) {
                      // 接收实时数据的场合，全量更新ES和Hbase
                      lastCaptured = cpKafka - cpHbase

                    } else {

                      lastCaptured = cpKafka - cpHbase

                    }
                  }

                  if (cpf != null && cpf.equals("lt")) {
                    ltHbase = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength).toString().toLong
                  }

                  if (lastCaptured > 0) {
                    if ((lastCaptured + ltHbase) < 12 * 60 * 60 && ltHbase != 0) {

                      lastCaptured = -1

                    }
                  }

                }

              } else {

                // 第一次入城-Start
                //如果captureTime大于当前时间，不更新到Hbase
                if (captureTime == 0) {
                  lastCaptured = -1
                }

                lastCaptured = 0
                // 第一次入城-END0

              }

            } else {
              lastCaptured = -1
            }
          }

          passInfo2.lastCaptured = lastCaptured.toInt
          // 初次入城-end

          // 判断是否为空
          if (passInfo2.id == null) {
            passInfo2.id = ""
          }
          if (passInfo2.locationuuid == null) {
            passInfo2.locationuuid = ""
          }
          if (passInfo2.levelId == null) {
            passInfo2.levelId = -1
          }
          if (passInfo2.yearId == null) {
            passInfo2.yearId = -1
          }
          if (passInfo2.modelId == null) {
            passInfo2.modelId = -1
          }
          if (passInfo2.brandId == null) {
            passInfo2.brandId = -1
          }
          if (passInfo2.plateNumber == null) {
            passInfo2.plateNumber = ""
          }
          if (passInfo2.regionCode == null) {
            passInfo2.regionCode = 0
          }
          if (passInfo2.captureTime == null) {
            passInfo2.captureTime = System.currentTimeMillis() / 1000l
          }
          if (passInfo2.nodeId == null) {
            passInfo2.nodeId = ""
          }
          if (passInfo2.partType == null) {
            passInfo2.partType = -1
          }
          if (passInfo2.colorId == null) {
            passInfo2.colorId = -1
          }
          if (passInfo2.directionId == null) {
            passInfo2.directionId = ""
          }
          if (passInfo2.plateTypeId == null) {
            passInfo2.plateTypeId = -1
          }
          if (passInfo2.plateCategoryId == null) {
            passInfo2.plateCategoryId = -1
          }
          if (passInfo2.lastCaptured == null) {
            passInfo2.lastCaptured = -1l
          }
          if (passInfo2.skylight == null) {
            passInfo2.skylight = -1
          }
          if (passInfo2.baggageHold == null) {
            passInfo2.baggageHold = -1
          }
          if (passInfo2.sprayWord == null) {
            passInfo2.sprayWord = -1
          }
          if (passInfo2.inspectionTag == null) {
            passInfo2.inspectionTag = -1
          }
          if (passInfo2.sunShadeLeft == null) {
            passInfo2.sunShadeLeft = -1
          }
          if (passInfo2.sunShadeRight == null) {
            passInfo2.sunShadeRight = -1
          }
          if (passInfo2.pendant == null) {
            passInfo2.pendant = -1
          }
          if (passInfo2.tissueBox == null) {
            passInfo2.tissueBox = -1
          }
          if (passInfo2.decoration == null) {
            passInfo2.decoration = -1
          }
          if (passInfo2.card == null) {
            passInfo2.card = -1
          }
          if (passInfo2.personLeft == null) {
            passInfo2.personLeft = -1
          }
          if (passInfo2.personRight == null) {
            passInfo2.personRight = -1
          }
          if (passInfo2.backBurnerTail == null) {
            passInfo2.backBurnerTail = -1
          }
          if (passInfo2.sendTime == null) {
            passInfo2.sendTime = System.currentTimeMillis() / 1000l
          }
          if (passInfo2.recPlateNumber == null) {
            passInfo2.recPlateNumber = ""
          }
          if (passInfo2.locationId == null) {
            passInfo2.locationId = ""
          }
          if (passInfo2.carBox == null) {
            passInfo2.carBox = ""
          }
          if (passInfo2.imageUrl == null) {
            passInfo2.imageUrl = ""
          }
          if (passInfo2.speed == null) {
            passInfo2.speed = -1
          }
          if (passInfo2.feature == null) {
            passInfo2.feature = ""
          }
          if (passInfo2.yearIdProb == null) {
            passInfo2.yearIdProb = 0
          }

          passInfo2

        })

        // 先处理消息
        if (!aa.isEmpty()) {

          // 初次入城入Hbase-Start
          var isAa = aa.filter { x => x.lastCaptured >= 0 }

          if (!isAa.isEmpty()) {

            var firstAa = isAa.map { x =>
              var last = PassinfoForLast(
                x.id,
                x.plateNumber,
                x.captureTime,
                x.lastCaptured)

              last
            }
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
            def convert(triple: (PassinfoForLast)) = {
              var input = triple
              var rowkey = triple.platenumber.reverse.toString()
              var id = triple.id
              var ct = triple.capturetime.toString()
              var lt = triple.lastCaptured.toString()
              var p = new Put(Bytes.toBytes(rowkey))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"), Bytes.toBytes(ct))
              p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lt"), Bytes.toBytes(lt))
              (new ImmutableBytesWritable, p)
            }

            var aaa = firstAa.map(convert)

            aaa.saveAsNewAPIHadoopDataset(job.getConfiguration())

            print("初次入城的数据量 ： " + firstAa.count() + "   ")

          } else {
            print("初次入城的数据量 ： " + 0 + "   ")
          }

          //          if (typeHbaseStr.equals("2")) {
          //
          //            var isAa = aa.filter { x => x.lastCaptured >= 0 }
          //
          //            if (!isAa.isEmpty()) {
          //
          //              isAa.foreachPartition { isAaR =>
          //                val hbaseConn = HBaseConnectManager.getConnet(zk_server_str)
          //                var table = hbaseConn.getTable(TableName.valueOf(hbaseTableNameForFirstCity));
          //
          //                var puts = Collections.synchronizedList(new ArrayList[Put]())
          //
          //                isAaR.foreach { x =>
          //                  var rowkey = x.plateNumber.reverse.toString()
          //                  var id = x.id
          //                  var ct = x.captureTime.toString()
          //                  var lt = x.lastCaptured.toString()
          //                  var p = new Put(Bytes.toBytes(rowkey))
          //                  p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id))
          //                  p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"), Bytes.toBytes(ct))
          //                  p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lt"), Bytes.toBytes(lt))
          //                  puts.add(p)
          //                }
          //
          //                table.put(puts)
          //                table.close()
          //
          //              }
          //            }
          //
          //            print("初次入城的数据量 ： " + isAa.count() + "   ")
          //
          //          }
          // 初次入城人Hbase-End

          // 历史数据推送kafka-Start

          if (true) {

            aa.foreachPartition(rdd => {
              if (!rdd.isEmpty) {
                rdd.foreach(record => {

                  val gsonEs = new Gson
                  var jsonHis = gsonEs.toJson(record)
                  kafkaProducer.value.send(kafkaPreData, jsonHis)
                })
              }
            })

          }
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
}