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
import org.elasticsearch.common.xcontent.XContentFactory

/**
 * 低级Api实现
 *
 * ES批量数据的写入实现
 */
object DataToES2 {

  def main(args: Array[String]) {

    // ES的节点地址
    var esNodeIp = "128.127.120.243"

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

    // ES
    //    sparkConf.set("es.index.auto.create", "true")
    //    sparkConf.set(ConfigurationOptions.ES_NODES, esNodeIp) //节点信息  

    // 自动把集群下的机器添加到列表中
    var settings: Settings = Settings.builder().put("cluster.name", "wuhanes").put("client.transport.sniff", true).build();

    var client = new PreBuiltTransportClient(settings);

    //添加集群IP列表
    if (esNodeIp.contains(",")) {

      esNodeIp.split(",").foreach { ip =>
        var transportAddress: TransportAddress = new InetSocketTransportAddress(InetAddresses.forString(ip), 9300);
        client.addTransportAddresses(transportAddress);
      }

    } else {

      var transportAddress: TransportAddress = new InetSocketTransportAddress(InetAddresses.forString(esNodeIp), 9300);
      client.addTransportAddresses(transportAddress);

    }

    import java.util.HashMap
    val indexSetting = new HashMap[String, Object]()
    indexSetting.put("number_of_shards", Integer.getInteger("4"))
    indexSetting.put("number_of_replicas", Integer.getInteger("4"))
    indexSetting.put("index.refresh_interval", "30s")

    //2:mappings  
    val builder1 = XContentFactory.jsonBuilder()
      .startObject()
      .field("dynamic", "stu")
      .startObject("properties")
      .startObject("id")
      .field("type", "integer")
      .field("store", "yes")
      .endObject()
      .startObject("name")
      .field("type", "string")
      .field("store", "yes")
      .field("index", "analyzed")
      .field("analyzer", "id")
      .endObject()
      .endObject()
      .endObject();

    val prepareCreate = client.admin().indices().prepareCreate("shb01")

    prepareCreate.setSettings(indexSetting).addMapping("stu", builder1).execute().actionGet();

    var sparkConf = new SparkConf().setAppName("DataToESAndHbase")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(30))

    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> "gid201704171159001",
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
          var A001 = passInfo2.id
          var A002 = UUID.nameUUIDFromBytes(id.getBytes()).toString().replaceAll("-", "");
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
          var A009 = passInfo2.captureTime
          if (passInfo2.captureTime == null) {
            A009 = 0l
          }
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
        var aaCount = aa.count()
        if (aaCount > 0) {

          // 入ES-Start
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

          println("aaES.Size() : " + aaEs.count())
          aaEs.foreachPartition { ap =>

            // 入ES-End

            val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            var nowDate = formatRdd.format(new Date())

            km.updateZKOffsets(rdd)
          }
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