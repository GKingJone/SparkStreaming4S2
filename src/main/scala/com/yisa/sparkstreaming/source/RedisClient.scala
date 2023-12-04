package com.yisa.sparkstreaming.source

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisSentinelPool
import java.util.HashSet
import redis.clients.jedis.HostAndPort

object RedisClient extends Serializable {
  //  lazy val SENTINEL_HOST = Config10.configs.get("sentinel_host")
  //  lazy val SENTINEL_PORT = Config10.configs.get("sentinel_port").toInt
  //  lazy val SENTINEL_NAME = Config10.configs.get("sentinel_name")

    val SENTINEL_HOST = "huangdao1"
    val SENTINEL_PORT = 26379
    val SENTINEL_NAME = "mymaster"
    val set: HashSet[String] = new HashSet[String]();
  
    val hosts = SENTINEL_HOST.split(",")
  
    hosts.foreach { host =>
      {
        set.add(String.valueOf(new HostAndPort(host, SENTINEL_PORT)))
  
      }
    }
  
    val redisTimeout = 30000
    var pool: JedisSentinelPool = new JedisSentinelPool(SENTINEL_NAME, set);
//
//  val redisHost = "db1"
//  val redisPort = 6379
//  val redisTimeout = 30000
//  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}