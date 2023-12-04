package com.yisa.sparkstreaming.source

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
/**
 * @author liliwei
 * @date  2016年9月9日
 * HBase 连接池
 */
object HBaseConnectManager {
  @volatile private var conn: Connection = null;
  @volatile private var config: Configuration = null;
  private val hbase_zookeeper_property_clientPort = "2181"
  private var hbase_zookeeper_quorum = "wh3"

  def getConfig(zkHostport: String): Configuration = {

    if (config == null) {
      synchronized {
        if (config == null) {
          try {
            Config10.initConfig(zkHostport)

            val configs = Config10.configs

            val ZK_HOSTS = configs.get("ZK_HOSTS")

            hbase_zookeeper_quorum = ZK_HOSTS
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
            config.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
          } catch {
            case ex: Exception => {
              println("IO Exception")
            }
          }
        }
      }
    }
    config
  }

  // 新API
  def getConnet(zkHostport: String): Connection = {

    if (conn == null) {
      synchronized {
        if (conn == null) {
          Config10.initConfig(zkHostport)

          val configs = Config10.configs

          val ZK_HOSTS = configs.get("ZK_HOSTS")

          val configuration = HBaseConfiguration.create();
          hbase_zookeeper_quorum = ZK_HOSTS

          configuration.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
          configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
          try {
            conn = ConnectionFactory.createConnection(configuration)
          } catch {
            case ex: Exception => {
              println("IO Exception")
            }
          }
        }
      }
    }
    conn
  }
}