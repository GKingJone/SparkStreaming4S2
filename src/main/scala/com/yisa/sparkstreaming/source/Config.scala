package com.yisa.sparkstreaming.source

import java.util.Properties
import java.io.FileInputStream
import com.yisa.wifi.zookeeper.ZookeeperUtil


object Config {
  
  // zookeeper工具类
  var zkUtil : ZookeeperUtil = null
  
  /*
   * spark app 名称
   */
  var APP_NAME = ""
  
  /*
   * 读取kafka数据的时间间隔(MS)
   */
  var READ_KAFKA_DATA_TIME = 0
  
  /*
   * zookeeper的数据路径 
   */
  var ZK_DATA_DIR = ""
  
  /*
   * kafka topic = analysis_passinfos
   */
  var KAFKA_TOPIC = ""
  
  /*
   * kafka group id
   */
  var KAFKA_GROUP_ID = ""
  
  /*
   * kafka broker id
   */
//  var KAFKA_BROKER_ID = ""
  
  /*
   * kafka offset id
   */
//  var KAFKA_OFFSET_ID = ""
  
  /*
   * spark插入hive数据的app名称 = SparkInsertToHive
   */
//  var Spark_Insert_TO_Hive = ""
  
  /*
   * spark配置hive的表目录 = hdfs://gpu10:8020/user/hive/warehouse
   */
  var SPARK_WARE_HOUSE_LOCATION = ""
  
  /*
   * spark 临时表 = tmp_pass_info
   */
  var SPARK_TMP_TABLE = ""
  
  /*
   * spark ui port
   */
//  var SPARK_UI_PORT = ""
  
  /*
   * hive动态表模式 = set  hive.exec.dynamic.partition.mode=nonstrict
   */
//  var HIVE_DYNAMIC_MODEL = ""
  
  /*
   * hive插入表的sql = insert into yisadata.pass_info_10 Select * from tmp_pass_info DISTRIBUTE BY dateid
   */
 var HIVE_INSERT_SQL = ""
 
 /*
  * conf.properties
  */
 var ZK_SERVER_UT = ""
 
 val ZK_TYPE = "spark_engine"
 
 def loadProperties():Unit = {
    val properties = new Properties()
    val path = "/app/conf.properties" //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    
    ZK_SERVER_UT = properties.getProperty("zk_server_ut").toString()
  }
  
  def main(args: Array[String]) = {
    initConfig("")
  }
  
  // 初期化配置文件
  def initConfig(zkServer : String) {
    
    if(zkUtil == null){
//      loadProperties()
      
      ZK_SERVER_UT = zkServer
    
      zkUtil = new ZookeeperUtil()
      
      // spark app 名称
      APP_NAME = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"APP_NAME").toString()
      
      // 读取kafka数据的时间间隔(M)
      READ_KAFKA_DATA_TIME = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"READ_KAFKA_DATA_TIME").toInt
      
      // zookeeper的数据路径 
      ZK_DATA_DIR = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"ZK_DATA_DIR").toString()
      
      KAFKA_GROUP_ID = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"KAFKA_GROUP_ID").toString()
      
      // kafka topic
      KAFKA_TOPIC = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"KAFKA_TOPIC").toString()
      
      // kafka topic
//      KAFKA_BROKER_ID = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"KAFKA_BROKER_ID").toString()
//      
//      // kafka topic
//      KAFKA_OFFSET_ID = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"KAFKA_OFFSET_ID").toString()
//      
//      // spark ui port
//      SPARK_UI_PORT = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"SPARK_UI_PORT").toString()
      
      // spark插入hive数据的app名称
//      Spark_Insert_TO_Hive = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"Spark_Insert_TO_Hive").toString()
      
      // spark配置hive的表目录
      SPARK_WARE_HOUSE_LOCATION = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"SPARK_WARE_HOUSE_LOCATION").toString()
      
      // spark 临时表
      SPARK_TMP_TABLE = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"SPARK_TMP_TABLE").toString()
      
      // hive动态表模式
//      HIVE_DYNAMIC_MODEL = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"HIVE_DYNAMIC_MODEL").toString()
      
      // hive插入表的sql
      HIVE_INSERT_SQL = zkUtil.getConfig(ZK_SERVER_UT,ZK_TYPE,"HIVE_INSERT_SQL").toString()
      
    }
  }
  
  def showString() : String = {
    
    val sb : StringBuffer = new StringBuffer()
    
    sb.append("DataToHive参数 : ").append("\n")
    
    sb.append("zookeeper服务器 : ").append(ZK_SERVER_UT).append("\n")
    
    sb.append("spark app 名称 : ").append(APP_NAME).append("\n")
    
    sb.append("读取kafka数据的时间间隔(M) : ").append(READ_KAFKA_DATA_TIME).append("\n")
    
    sb.append("zookeeper的数据路径  : ").append(ZK_DATA_DIR).append("\n")
    
    sb.append("Kafka group id  : ").append(KAFKA_GROUP_ID).append("\n")
    
    sb.append("Kafka topic  : ").append(KAFKA_TOPIC).append("\n")
    
    sb.append("spark配置hive的表目录  : ").append(SPARK_WARE_HOUSE_LOCATION).append("\n")
    
    sb.append("spark 临时表  : ").append(SPARK_TMP_TABLE).append("\n")
    
    sb.append("hive插入表的sql  : ").append(HIVE_INSERT_SQL).append("\n")
    
    sb.toString()
    
  }
  
}