package com.myspark.kafka_sparkstreaming

import java.util.Objects

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}


object InternalRedisClient extends Serializable {

  @transient
  private var pool: JedisPool = _

  def makePool(redisHost: String = "localhost",
               redisPort: Int = 27890,
               redisTimeout: Int = 3000,
               maxTotal: Int = 100,
               maxIdle: Int = 8,
               minIdle: Int = 2,
               testOnBorrow: Boolean = true,
               testOnReturn: Boolean = false,
               maxWaitMillis: Long = 10000): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

      sys.addShutdownHook {
        pool.destroy()
      }
    }
  }

  private def getPool: JedisPool = {
    Objects.requireNonNull(pool)
  }

  def getResource: Jedis = {
    getPool.getResource
  }

  def returnResource(jedis: Jedis): Unit = {

    if (jedis != null) {
      try{
        jedis.close()
      }catch {
        case e: Exception => println(e)
      }
    }
  }
}
