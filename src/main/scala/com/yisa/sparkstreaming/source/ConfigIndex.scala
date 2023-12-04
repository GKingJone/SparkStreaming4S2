package com.yisa.sparkstreaming.source

import java.util.Properties
import java.io.FileInputStream
import com.yisa.wifi.zookeeper.ZookeeperUtil

object ConfigIndex {

  // zookeeper工具类
  var zkUtil: ZookeeperUtil = null
  var configs: java.util.Map[String, String] = null

  /*
   * 读取kafka数据的时间间隔(MS)
   */
  var preReadKafkaBatchTime = 0
  
  /*
   * 初次入城的Hbase表
   */
  var preHbaseTableNameForFirstCity = ""
  
  /*
   * 消费Kafka Group Id
   */
  var preGroupId = ""
  
  /*
   * Spark UI prot 
   */
  var preSparkUI = ""
  
  /*
   * 消费Kafka Topic Name
   */
  var preKafkaConsumerTopicName = ""
  
  /*
   * 生产Kafka Topic Name
   */
  var preKafkaProducerTopicName = ""
  
  /*
   * 天脑数据库URL
   */
  var jdbcGBUrl = ""
  
  /*
   * 天脑用户名
   */
  var jdbcGBUsername = ""
  
  /*
   * 天脑用户密码
   */
  var jdbcGBPassword = ""
  
  /*
   * Kafka Broker ID
   */
  var comKafkaBrokerIDs = ""
  
  /*
   * Spark执行的并发数
   */
  var preConcurrentSize = 100
  
  /*
  * conf.properties
  */
  var ZK_SERVER_UT = ""

  // 初期化配置文件
  def initConfig(zkServer: String, zkType : String) {

    if (zkUtil == null) {

      synchronized {
        if (zkUtil == null) {

          ZK_SERVER_UT = zkServer

          zkUtil = new ZookeeperUtil()
          
          configs = zkUtil.getAllConfig(ZK_SERVER_UT, zkType, false)

          // 读取kafka数据的时间间隔(MS)
          preReadKafkaBatchTime = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preReadKafkaBatchTime").toInt

          // 初次入城的Hbase表
          preHbaseTableNameForFirstCity = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preHbaseTableNameForFirstCity").toString()

          // 消费Kafka Group Id
          preGroupId = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preGroupId").toString()

          // Spark UI prot 
          preSparkUI = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preSparkUI").toString()

          // 消费Kafka Topic Name
          preKafkaConsumerTopicName = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preKafkaConsumerTopicName").toString()

          // 生产Kafka Topic Name
          preKafkaProducerTopicName = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preKafkaProducerTopicName").toString()

          // 天脑数据库URL
          jdbcGBUrl = zkUtil.getConfig(ZK_SERVER_UT, zkType, "jdbcGBUrl").toString()

          // 天脑用户名
          jdbcGBUsername = zkUtil.getConfig(ZK_SERVER_UT, zkType, "jdbcGBUsername").toString()

          // 天脑用户密码
          jdbcGBPassword = zkUtil.getConfig(ZK_SERVER_UT, zkType, "jdbcGBPassword").toString()

          // Kafka Broker ID
          comKafkaBrokerIDs = zkUtil.getConfig(ZK_SERVER_UT, zkType, "comKafkaBrokerIDs").toString()

          // Spark执行的并发数
          preConcurrentSize = zkUtil.getConfig(ZK_SERVER_UT, zkType, "preConcurrentSize").toInt
  
        }
      }
    }
  }

  def showString(): String = {

    val sb: StringBuffer = new StringBuffer()

    sb.append("ConfigIndex参数 : ").append("\n")

    sb.append("zookeeper服务器 : ").append(ZK_SERVER_UT).append("\n")

    sb.append("预处理-读取kafka数据的时间间隔(MS) : ").append(preReadKafkaBatchTime).append("\n")

    sb.append("预处理-初次入城的Hbase表 : ").append(preHbaseTableNameForFirstCity).append("\n")

    sb.append("预处理-消费Kafka Group Id : ").append(preGroupId).append("\n")

    sb.append("预处理-Spark UI prot : ").append(preSparkUI).append("\n")

    sb.append("预处理-消费识别新接口Kafka Topic Name : ").append(preKafkaConsumerTopicName).append("\n")

    sb.append("预处理-生产Kafka Topic Name : ").append(preKafkaProducerTopicName).append("\n")
    
    sb.append("预处理-天脑数据库URL : ").append(jdbcGBUrl).append("\n")
    
    sb.append("预处理-天脑用户名 : ").append(jdbcGBUsername).append("\n")
    
    sb.append("预处理-天脑用户密码 : ").append(jdbcGBPassword).append("\n")
    
    sb.append("预处理-Kafka Broker ID : ").append(comKafkaBrokerIDs).append("\n")
    
    sb.append("预处理-Spark执行的并发数 : ").append(preConcurrentSize).append("\n")

    sb.toString()

  }

}