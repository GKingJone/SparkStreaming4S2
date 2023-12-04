package com.yisa.sparkstreaming.source

import com.yisa.wifi.zookeeper.ZookeeperUtil
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisSentinelPool
import redis.clients.jedis.Jedis
import java.util.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author 李立伟
 * @date 2017年2月21日
 * @version V1.0
 */
object RedisConnectionPool {

  private var pool: JedisSentinelPool = null

  def initJedisSentinelPool(zkHostport: String): JedisSentinelPool = {

    if (pool == null) {
      synchronized {
        if (pool == null) {

          Config10.initConfig(zkHostport)
          var configs = Config10.configs
          
          var set = new HashSet[String]()
          val SENTINEL_NAME = configs.get("sentinel_name");
          val SENTINEL_HOST = configs.get("sentinel_host");
          val SENTINEL_PORT = Integer.valueOf(configs.get("sentinel_port"));

          var hosts = SENTINEL_HOST.split(",");
          hosts.foreach { host =>
            set.add(String.valueOf(new HostAndPort(host, SENTINEL_PORT)))
          }
          pool = new JedisSentinelPool(SENTINEL_NAME, set)
        }
      }
    }
    pool
  }

  /**
   * 获取Jedis实例
   *
   * @return
   */
  def getJedis(zkHostport: String): Jedis = {

    if (pool == null) {
      synchronized {
        initJedisSentinelPool(zkHostport)
      }
    }

    return pool.getResource();

  }

  /**
   * 返还到连接池
   *
   * @param pool
   * @param redis
   */
  def returnResource(jedis: Jedis) {

    if (jedis != null) {
      pool.returnResource(jedis);
    } else {
      pool.returnBrokenResource(jedis);
    }

  }
  
  
 
  
  
  /**
	 * 保存HashMap类型的值
	 *
	 * @param key
	 * @param map
	 * @throws Exception 
	 */
	def   hmset( key:String,map: java.util.Map[String, String]):Unit= {
		
		 var jedis:Jedis = null;
		try {
			jedis = pool.getResource();
			jedis.hmset(key, map);
		} catch  {
		   case e: Exception => {
		     throw e
		   }
		} finally {
			returnResource(jedis);
		}
		
	}
  
  
  
  
  
  
  
  

  def set(db: Int, jedis: Jedis, key: String, value: String, daysToExpired: Int) {
    jedis.select(db)
    jedis.set(key, value);
    if (daysToExpired != -1) {
      jedis.expire(key, daysToExpired * 24 * 60 * 60);
    }

  }

  def get(db: Int, jedis: Jedis, key: String): String = {
    jedis.select(db)
    return jedis.get(key);

  }

}