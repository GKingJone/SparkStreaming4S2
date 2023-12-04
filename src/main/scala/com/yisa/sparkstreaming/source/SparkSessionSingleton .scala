package com.yisa.sparkstreaming.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * @author liliwei
 * @date  2016年9月9日
 */
object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}