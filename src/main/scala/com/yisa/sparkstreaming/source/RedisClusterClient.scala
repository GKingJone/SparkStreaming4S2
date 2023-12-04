package com.yisa.sparkstreaming.source

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.util.HashSet
import redis.clients.jedis.JedisSentinelPool
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisPoolConfig

object RedisClusterClient extends Serializable {

  val port = 6379

  val hosts = "172.22.0.68,172.22.0.69,172.22.0.70".split(",")
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
  
  
  lazy val pool: JedisCluster = new JedisCluster(nodes,config)
  
  
  
  
  

}