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
import com.yisa.sparkstreaming.source.RedisConnectionPool
import redis.clients.jedis.Jedis

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
object DataToPreAnalysis {

  def main(args: Array[String]) {

    // hbase初次入城的表名
    var hbaseTableNameForFirstCity: String = "last_captured1"

    var groupId = "gid2017050100001"

    var kafkaPreData = "pre_analysis_passinfos"

    var redis_select = 3
    
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

    var sparkConf = new SparkConf().setAppName("DataToPreAnalysis")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14048")

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(10))

    //  producer
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", Config10.KAFKA_BROKER_ID)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
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

          var esId: String = "" // ES ID Hbase Rowkey
          var locationuuid: String = "" // 卡口
          var modelId: Int = -1 // 车辆型号
          var brandId: Int = -1 // 车辆品牌
          var regionCode: Int = 0 // 行政区划编码
          var plateTypeId: Int = -1 // 车牌类型
          var lastCaptured: Long = 0 // 初次入城时间

          try {
            var gson = new Gson
            var mapType = new TypeToken[PassInfo2] {}.getType
            passInfo2 = gson.fromJson[PassInfo2](line, mapType)
          } catch {
            case ex: Exception => {
              println(ex.getMessage + "  json数据接收异常 ：" + line)
            }
          }

          // 实例化
          esId = UUID.randomUUID().toString().replace("-", "")
          var sBuilder = new StringBuilder();
          sBuilder.append(passInfo2.nodeId).append("_").append(passInfo2.locationId);
          var id = sBuilder.toString();
          locationuuid = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "")

          passInfo2.id = esId
          passInfo2.locationuuid = locationuuid

          // 获取redis连接
          var redisClient: Jedis = null
          try {

            RedisConnectionPool.initJedisSentinelPool(zk_server_str)
            redisClient = RedisConnectionPool.getJedis(zk_server_str)
            redisClient.select(redis_select)

          } catch {
            case ex: Exception => {
              println(ex.getMessage + "Redis连接失败！程序退出！")

              System.exit(1)
            }
          }

          // 车辆型号,车辆品牌
          var partType = passInfo2.partType
          var yearId = passInfo2.yearId
          if (partType != null && !"".equals(partType)) {

            if (partType == 1) {
              var key = "vehicle_head_" + yearId
              var rsmap = redisClient.hmget(key, "brand_id", "model_id")
              if (rsmap != null) {
                if (rsmap.get(0) != null)
                  brandId = rsmap.get(0).toInt
                if (rsmap.get(1) != null)
                  modelId = rsmap.get(1).toInt
              }
            } else if (partType == 2) {
              var key = "vehicle_tail_" + yearId
              var rsmap = redisClient.hmget(key, "brand_id", "model_id")
              if (rsmap != null) {
                if (rsmap.get(0) != null)
                  brandId = rsmap.get(0).toInt
                if (rsmap.get(1) != null)
                  modelId = rsmap.get(1).toInt
              }
            }
          }
          passInfo2.brandId = brandId
          passInfo2.modelId = modelId

          // 行政区划编码
          var nodeId = passInfo2.nodeId
          var loc_id = passInfo2.locationId
          var regionKey = "location_" + nodeId + "_" + loc_id
          var regionMap = redisClient.hmget(regionKey, "region_code")
          if (regionMap != null) {
            if (regionMap.get(0) != null)
              regionCode = regionMap.get(0).toInt
          }
          passInfo2.regionCode = regionCode

          // 车牌类型
          var plateCategoryId = passInfo2.plateCategoryId
          var plateKey = "plate_type_" + plateCategoryId
          var plateMap = redisClient.hmget(plateKey, "plate_type_id")
          if (plateMap != null) {
            if (plateMap.get(0) != null)
              plateTypeId = plateMap.get(0).toInt
          }

          passInfo2.plateTypeId = plateTypeId
          
          RedisConnectionPool.returnResource(redisClient)

          if (true) {
            // 初次入城-start
            // lastCaptured = -1表示不满足初次入城的条件
            // 目的：求出正常情况的lastCaptured
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
              case e: Exception => println(e.getMessage + "日期格式转换错误")
            }
            var plateNumber = passInfo2.plateNumber // 车牌
            if (plateNumber != null && !"".equals(plateNumber) && !plateNumber.contains("无")
              && !plateNumber.contains("未") && plateNumber.length() >= 6 && plateNumber.length() <= 10) {

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
                    if ((lastCaptured + ltHbase) < 12 * 60 && ltHbase != 0) {

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

          passInfo2

        })

        // 先处理消息
        if (!aa.isEmpty()) {

          // 初次入城入Hbase-Start
          if (true) {

            var isAa = aa.filter { x => x.lastCaptured >= 0 }

            if (!isAa.isEmpty()) {
              
              var firstAa = aa.map { x =>  
                var last = PassinfoForLast(
                    x.id,
                    x.plateNumber,
                    x.captureTime,
                    x.lastCaptured
                    )
                    
                    last
              }
              // 新API
              sc.hadoopConfiguration.set("hbase.zookeeper.quorum", zk_old)
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

          }
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