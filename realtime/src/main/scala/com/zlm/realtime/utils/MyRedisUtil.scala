package com.zlm.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util.Properties

/**
 * @author Harbour 
 * @date 2021-04-02 13:10
 */
object MyRedisUtil {

    private var jedisPool: JedisPool = null

    def getJedisClient: Jedis = {
        if (jedisPool == null) {
            build()
        }
        jedisPool.getResource
    }

    def build(): Unit = {
        val jedisConfig: JedisPoolConfig = new JedisPoolConfig
        jedisConfig.setMaxTotal(100) // 最大连接数
        jedisConfig.setMaxIdle(20) // 最大空闲
        jedisConfig.setMinIdle(20) // 最大空闲
        jedisConfig.setBlockWhenExhausted(true) // 忙碌时是否等待
        jedisConfig.setMaxWaitMillis(5000) // 等待时长
        jedisConfig.setTestOnBorrow(true) // 每次获取连接进行测试

        val prop: Properties = new Properties()
        prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties"))
        val host: String = prop.getProperty("redis.host")
        val port: String = prop.getProperty("redis.port")

        this.jedisPool = new JedisPool(jedisConfig, host, port.toInt)
    }

    def main(args: Array[String]): Unit = {
        val client: Jedis = getJedisClient
        println(client.ping())
        client.close()
    }

}
