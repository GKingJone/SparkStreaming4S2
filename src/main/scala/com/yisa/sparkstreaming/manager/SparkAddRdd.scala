package com.yisa.sparkstreaming.manager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

object SparkAddRdd {
  
  def main (args : Array[String]){
    

    val hdfsPath = "hdfs://sichuan0:8020"
    val dataPath = "/user/hive/warehouse/yisadata.db/pass_info/"
    val tableName = "cacheTable"
    
    val sparkSession = SparkSession
      .builder()
      .appName("SparkAddRdd")
      .getOrCreate()
      
      
    var initDataSet : Dataset[Row] = loadAllData(sparkSession,dataPath)
    
    initDataSet.createOrReplaceTempView(tableName)
    
    sparkSession.catalog.cacheTable(tableName)
    
    runSqlCount(sparkSession)
    var i = 1
    var isStop = true
    
    while(isStop){
      
      runSql(sparkSession)
      
      var date = getLCacheDataDateid(i)
      
      var addDateSet : Dataset[Row] = loadAddData(sparkSession, dataPath, date)
      
      initDataSet = initDataSet.union(addDateSet)
       
      sparkSession.catalog.uncacheTable(tableName)
      sparkSession.catalog.dropTempView(tableName)
      sparkSession.catalog.clearCache()
      Thread.sleep(10000)
    
      initDataSet.createOrReplaceTempView(tableName)
      
      sparkSession.catalog.cacheTable(tableName)
    
      runSqlCount(sparkSession)
      
      if(i == 10){
        isStop = false
      }
      
      i = i + 1
    }
    
    runSql(sparkSession)
    
    sparkSession.stop()
    
  }
  
  def loadAllData(sparkSession: SparkSession, dataPath : String): Dataset[Row] = {

    val loadData = sparkSession.read.option("mergeSchema", "true").parquet(dataPath + "20161101")

    return loadData
  }
  
  def loadAddData(sparkSession: SparkSession, dataPath : String, date : String): Dataset[Row] = {

    val loadData = sparkSession.read.option("mergeSchema", "true").parquet(dataPath + date)

    return loadData
  }
  
  def runSql(sparkSession : SparkSession){
    
    var start = System.currentTimeMillis()
    var rddSql = sparkSession.sql("Select plateNumber from cacheTable where dateid = 20161101 group by plateNumber")
   
    var count = rddSql.count()
    var end = System.currentTimeMillis()
    
    println("时间(S) ：" + (end - start)/1000 + " 数量 ：" + count)
  }
  
  def runSqlCount(sparkSession : SparkSession){
    
    var start = System.currentTimeMillis()
    var rddSql = sparkSession.sql("Select count(*) from cacheTable")
    var count = rddSql.count()
    var end = System.currentTimeMillis()
    
    println("时间(S) ：" + (end - start)/1000 + " 数量 ：" + count)
    
  }
  
  def getLCacheDataDateid(days: Int): String = {
    var cal = Calendar.getInstance();
    cal.setTime(new Date());
    cal.add(Calendar.DAY_OF_MONTH, -days);
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(cal.getTime())
  }
}