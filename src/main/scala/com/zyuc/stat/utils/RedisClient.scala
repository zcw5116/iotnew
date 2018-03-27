package com.zyuc.stat.utils

import com.zyuc.stat.properties.RedisProperties
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by slview on 17-6-7.
  */
object RedisClient extends Serializable {
  private var MAX_IDLE: Int = 200
  private var TIMEOUT: Int = 10000
  private var TEST_ON_BORROW: Boolean = true

  lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxIdle(MAX_IDLE)
    config.setTestOnBorrow(TEST_ON_BORROW)
    config
  }

  lazy val pool = new JedisPool(config, RedisProperties.REDIS_SERVER,
    RedisProperties.REDIS_PORT, TIMEOUT)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}

