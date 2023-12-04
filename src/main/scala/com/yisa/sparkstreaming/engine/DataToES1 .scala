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

/**
 * 低级Api实现
 *
 * 初次入城实现
 */
object DataToES1 {

  def main(args: Array[String]) {

    var hbaseTableName: String = "last_captured1"

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

    var sparkConf = new SparkConf().setAppName("DataToES")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.ui.port", "14046")

    var sc = new SparkContext(sparkConf)
    var ssc = new StreamingContext(sc, Seconds(30))

    println("低级API实现")
    println(Config10.showString())
    println("Spark Streaming从每个分区，每秒读取的数据量 : " + sc.getConf.get("spark.streaming.kafka.maxRatePerPartition"))

    val topics = Config10.KAFKA_TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> Config10.KAFKA_BROKER_ID,
      "group.id" -> "gid2017032800003",
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
              var t = d.getTime()/1000
              captureTime = t
            }
          } catch {
            case e: Exception => println("cdr parse timestamp wrong")
          }

          // 查询Hbase
          val hbaseConn = HBaseConnectManager.getConnet(zk_server_str)
          val table = hbaseConn.getTable(TableName.valueOf(hbaseTableName));
          var get = new Get(Bytes.toBytes(passInfo2.plateNumber.reverse.toString()));

          var lastCaptured: Long = 0

          var result = table.get(get)

          if (result != null && result.size() > 0) {
            var cellList = result.listCells()
            for (i <- 0 to (cellList.size() - 1)) {
              var c = cellList.get(i)
              var cpf = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength())
              if (cpf != null && cpf.equals("ct")) {
                var cpv = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength).toString().toLong
                var cpvN = captureTime
//                println(cpv + "  ---  " + cpvN + "  --- " + (cpvN - cpv))
                if (cpv < cpvN) {
                  lastCaptured = cpvN - cpv
                } else {
                  lastCaptured = 0
                }
              }
            }
          } else {
            lastCaptured = 1
          }

          table.close()

          var inputBean = PassinfoForLast.apply(
              passInfo2.id,
            passInfo2.plateNumber,
            captureTime,
            lastCaptured)

          inputBean

        })

        // 先处理消息
        var aaCount = aa.count()
        if (aaCount > 0) {
          // 新API

          sc.hadoopConfiguration.set("hbase.zookeeper.quorum", zk_server_str)
          sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
          sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

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
            var id = ""
            var ct = triple.capturetime.toString()
            var lt = triple.lastCaptured.toString()
            var p = new Put(Bytes.toBytes(rowkey))
            p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id))
            p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"), Bytes.toBytes(ct))
            p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lt"), Bytes.toBytes(lt))
            (new ImmutableBytesWritable, p)
          }

          var aaa = aa.filter { x => x.lastCaptured > 0 }.map(convert)

          aaa.saveAsNewAPIHadoopDataset(job.getConfiguration())

          val formatRdd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          var nowDate = formatRdd.format(new Date())

          println(nowDate + " rdd的长度 : " + aaCount + " -- 初次入城的数据量 ：" + aaa.count())

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