package com.yisa.sparkstreaming.merge

import org.apache.spark.sql.SparkSession
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.commons.cli.MissingOptionException
import org.apache.commons.cli.PosixParser
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import org.apache.commons.cli.CommandLine
import com.yisa.sparkstreaming.source.Config10
import java.util.Date
import org.apache.spark.sql.AnalysisException
import com.yisa.sparkstreaming.source.SparkSessionSingletonModel
import org.apache.spark.sql.SaveMode

object HiveMoveBySpark {

  def main(args: Array[String]) = {

    var cmd: CommandLine = null

    val options: Options = new Options()

    try {

      var day: Option = new Option("day", true, "输入日期！")

      day.setRequired(false)

      options.addOption(day)

      val parser: PosixParser = new PosixParser()

      cmd = parser.parse(options, args)

    } catch {

      case ex: MissingOptionException => {

        println(ex)

        println("--zk_server <value> : " + options.getOption("zk_server").getDescription)

        System.exit(1)
      }
    }

    val warehouseLocation = "hdfs://gpu10:8020/user/hive/warehouse/"

    val spark = SparkSession
      .builder()
      .appName("HiveMergeBySpark")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val day = cmd.getOptionValue("day").toString().toInt

    val data1 = spark.read.option("mergeSchema", "true").parquet("hdfs://gpu10:8020/user/hive/warehouse/yisadata.db/pass_info/dateid=" + day)
    data1.createOrReplaceTempView("pass_info_tmp")

    print("data1 : " + data1.count())
    
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("insert into yisadata.pass_info partition(dateid=" + day + ") select * from pass_info_tmp DISTRIBUTE BY yearid%100")

    spark.stop();
  }

  def getNowDay(): Int = {

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var nowDay = dateFormat.format(new Date()).toInt

    nowDay

  }

  def getOldDay(): Int = {

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime()).toInt

    yesterday

  }
}