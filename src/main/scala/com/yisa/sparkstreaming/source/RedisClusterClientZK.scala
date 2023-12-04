package com.yisa.sparkstreaming.source

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.util.HashSet
import redis.clients.jedis.JedisSentinelPool
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisPoolConfig

object RedisClusterClientZK extends Serializable {

  @volatile private var pool: JedisCluster = null;

  def getJedisCluster(zkHostport: String): JedisCluster = {

    if (pool == null) {
      synchronized {
        if (pool == null) {
          
          Config10.initConfig(zkHostport)

          val configs = Config10.configs

          val port = configs.get("Redis_Cluster_Port").toInt

          val hosts =configs.get("Redis_Cluster").split(",")
          val nodes: HashSet[HostAndPort] = new HashSet[HostAndPort]();
          hosts.foreach { host =>
            {
              nodes.add(new HostAndPort(host, port))
            }
          }
          val config: JedisPoolConfig = new JedisPoolConfig();
          config.setMaxTotal(60000); //设置最大连接数  
          config.setMaxIdle(1000); //设置最大空闲数 
          config.setMaxWaitMillis(3000); //设置超时时间  
          config.setTestOnBorrow(true);
          //注意：这里超时时间不要太短，他会有超时重试机制。而且其他像httpclient、dubbo等RPC框架也要注意这点

          pool = new JedisCluster(nodes, config)
        }
      }
    }
    pool
  }

}