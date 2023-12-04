package com.yisa.sparkstreaming.source

import org.apache.spark.sql.SparkSession

/*
 * 实例化一个单例模式的SparkSession
 */
object SparkSessionSingletonModel {
  
  @transient private var instance : SparkSession = _
  
  def getInstance(warehouseLocation : String) : SparkSession = {
    
    if(instance == null){
      
      instance = SparkSession.builder()
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .config("spark.sql.broadcastTimeout", 1000)
          .enableHiveSupport()
          .getOrCreate()
    }
    
    return instance
  }
  
  @transient private var instanceP : SparkSession = _
  
  def getInstanceP(warehouseLocation : String) : SparkSession = {
    
    if(instance == null){
      
      instance = SparkSession.builder()
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .config("spark.sql.broadcastTimeout", 1000)
          .getOrCreate()
    }
    
    return instance
  }
}