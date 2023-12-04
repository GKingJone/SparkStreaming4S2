package com.yisa.sparkstreaming.engine

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.PosixParser
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
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
import com.yisa.sparkstreaming.source.Config10

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import com.yisa.sparkstreaming.model.PassinfoForLast
import com.yisa.sparkstreaming.source.HBaseConnectManager
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.Cell
import java.util.UUID
import com.yisa.sparkstreaming.model.PassInfoForFirstCity
import com.yisa.sparkstreaming.model.PassInfoForEs
import com.yisa.sparkstreaming.source.ESClient
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.search.sort.SortOrder

/**
 * 低级Api实现
 *
 * 初次入城实现
 */
object DataToFirstCity {

  def main(args: Array[String]) {

    var hbaseTableName: String = "last_captured1"

    // ES的节点地址
    var esNodeIp = "128.127.120.243"

    // ES的数据库
    var esDataBaseName = "last_captured"

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

    var sparkConf = new SparkConf().setAppName("DataToFirstCityForHistoryData")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14045")

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(30))

    Config10.KAFKA_TOPIC = "history_passinfos"
    val topics = Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> "gid2017042400001",
      "auto.offset.reset" -> "largest")

    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

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

          var format = new SimpleDateFormat("yyyyMMddHHmmss")

          var captureTime = 0L
          var plateNumber = passInfo2.plateNumber
          var esId = passInfo2.id

          // 接收的数据为实时数据的情况为1
          // 接收的数据为历史数据的情况为0
          var isAll = 1

          // lastCaptured = -1的场合，数据不做任何处理

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

          var lastCaptured: Int = passInfo2.lastCaptured

          if (plateNumber != null && !"".equals(plateNumber) && !plateNumber.contains("无")
            && !plateNumber.contains("未") && plateNumber.length() >= 6 && plateNumber.length() <= 10) {

            // 过滤时间差在6小时以内的数据，防止频繁查询与更新
            // 满足条件的数据需要从ES取出ESID，根据ESID取出HBase存储的数据
            // 用Hbase的数据覆盖ES和HBase
            // 注释：满足条件的数据（查询当前数据之后的数据，并保证查询的数据lastcaptured）
            if (lastCaptured > (-6 * 60)) {

              var client = ESClient.getInstance("", esNodeIp)
              var queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name.keyword", "SparkToEs666"))

              var rangeBuilder = QueryBuilders.rangeQuery("age").from(67).to(78)

              var searchResponse = client.prepareSearch("gaoxl4").setTypes("info")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setPostFilter(rangeBuilder)
                .addSort("age", SortOrder.ASC)
                .setSize(1)
                .execute()
                .actionGet()

              var hits = searchResponse.getHits
              System.out.println("查询到记录数=" + hits.getTotalHits());
              var searchHists = hits.getHits
              if (searchHists.length > 0) {
                var a = 0
                for (a <- 0 until searchHists.length) {
                  var hit = searchHists(a)
                  var m = hit.getSource()
                  println(hit.getId)
                  var id = m.get("age")
                  println(id)
                }
              }
              client.close()
            } else {

              // -1不做任何操作
              lastCaptured = -1

            }

          } else {

            // 不满足条件的车牌不更新Hbase

            lastCaptured = -1

          }

          // 实例化
          var A001 = esId
          var A009 = captureTime
          var A017 = lastCaptured.toInt
          var isALLData = isAll

          var inputBean = PassinfoForLast(A001,plateNumber, captureTime, lastCaptured)

          inputBean

        })

        // 先处理消息
        if (!aa.isEmpty()) {

          // 全量更新ES-Start
          if (true) {

            var ac = aa.filter { x => x.lastCaptured > 0 }
            ac.foreachPartition { ap =>

              var client = ESClient.getInstance(esDataBaseName, esNodeIp)

              var queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name.keyword", "SparkToEs666"))

              var rangeBuilder = QueryBuilders.rangeQuery("age").from(67).to(78)

              var searchResponse = client.prepareSearch("gaoxl4").setTypes("info")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setPostFilter(rangeBuilder)
                .addSort("age", SortOrder.ASC)
                .setSize(1)
                .execute()
                .actionGet()

              var hits = searchResponse.getHits
              System.out.println("查询到记录数=" + hits.getTotalHits());
              var searchHists = hits.getHits
              if (searchHists.length > 0) {
                var a = 0
                for (a <- 0 until searchHists.length) {
                  var hit = searchHists(a)
                  var m = hit.getSource()
                  println(hit.getId)
                  var id = m.get("age")
                  println(id)
                }
              }
            }

            val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            var nowDate = formatRdd.format(new Date())

            println(nowDate + " rdd的长度 : " + ac.count())

          }
          // 全量更新ES-END

          // 再更新offsets
          km.updateZKOffsets(rdd)
        }

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