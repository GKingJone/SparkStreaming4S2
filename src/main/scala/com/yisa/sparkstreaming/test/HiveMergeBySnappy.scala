package com.yisa.sparkstreaming.test

import org.apache.spark.sql.SparkSession
import java.util.Calendar
import java.text.SimpleDateFormat

object HiveMergeBySnappy {

  def main( args : Array[String]) = {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "hdfs://sichuan0:8020/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("HiveMergeBySpark")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("set hive.exec.compress.output=true")
    sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    
      for(i <- 1 to 10) {
        
        try{
          sql("insert into yisadata.pass_info_merge partition(dateid,locationid) select * from yisadata.pass_info where dateId = " + getOldDay(i) + " distribute by locationId")
          sql("alter table yisadata.pass_info drop partition(dateId = " + getOldDay(i) + ")")
          
          println("数据合并和历史数据删除！" + getOldDay(i))
        }  catch {
          case e: Exception => {
            println("分区不存在！" + getOldDay(i))
            println(e.getMessage)
          }
        }
        
      } 
      
    spark.stop();
  }
  
  def getOldDay(num : Int):Int={
    
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-num)
    var yesterday=dateFormat.format(cal.getTime()).toInt
    
    yesterday
    
  } 
}