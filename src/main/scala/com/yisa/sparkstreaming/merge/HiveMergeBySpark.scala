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

object HiveMergeBySpark {

  def main(args: Array[String]) = {

    var cmd: CommandLine = null

    val options: Options = new Options()

    try {

      var zkServer: Option = new Option("zk_server", true, "输入zookeeper服务器地址")

      zkServer.setRequired(true)

      options.addOption(zkServer)

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

    var zk_server_str = ""
    if (cmd != null && cmd.hasOption("zk_server")) {
      zk_server_str = cmd.getOptionValue("zk_server")
    }

    Config10.initConfig(zk_server_str)

    val warehouseLocation = Config10.SPARK_WARE_HOUSE_LOCATION
    
    val spark = SparkSession
      .builder()
      .appName("HiveMergeBySpark")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    
    var count = 0
    
    try {
      
      count = sql("select solrid from yisadata.pass_info where dateId = " + getOldDay()).count().toInt
      
    } catch {

      case ex: AnalysisException => {

        println(ex)

        spark.stop();
        
        System.exit(1)
      }
    }

    var day = getOldDay();
    if (cmd != null && cmd.hasOption("day")) {
      day = cmd.getOptionValue("day").toString().toInt
    }
    println("当前合并的日期" + day)
    
    if(count > 0){
      sql("set hive.exec.dynamic.partition.mode=nonstrict")
      // 合并pass_info数据 版本5
//      sql("insert into yisadata.pass_info_11 partition(dateid) select * from yisadata.pass_info where dateId = " + day +  " DISTRIBUTE BY colorid")
//      sql("alter table yisadata.pass_info drop partition(dateId = " + day + ")")
//      sql("insert into yisadata.pass_info partition(dateid) select * from yisadata.pass_info_11 where dateId = " + day +  " DISTRIBUTE BY colorid")
      
      // 合并pass_info数据 版本6
      println("insert into yisadata.pass_info_11 partition(dateid) select * from yisadata.pass_info where dateId = " + day +  " DISTRIBUTE BY yearid%100")
      println("alter table yisadata.pass_info drop partition(dateId = " + day + ")")
      println("insert into yisadata.pass_info partition(dateid) select * from yisadata.pass_info_11 where dateId = " + day +  " DISTRIBUTE BY yearid%100")
      println("alter table yisadata.pass_info_11 drop partition(dateId = " + day + ")")
      
      sql("insert into yisadata.pass_info_11 partition(dateid) select * from yisadata.pass_info where dateId = " + day +  " DISTRIBUTE BY yearid%100")
      sql("alter table yisadata.pass_info drop partition(dateId = " + day + ")")
      sql("insert into yisadata.pass_info partition(dateid) select * from yisadata.pass_info_11 where dateId = " + day +  " DISTRIBUTE BY yearid%100")
      sql("alter table yisadata.pass_info_11 drop partition(dateId = " + day + ")")
      
      println("数据合并和历史数据删除！" + day)
    }

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