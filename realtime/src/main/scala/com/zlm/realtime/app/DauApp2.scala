package com.zlm.realtime.app

import com.zlm.realtime.utils.{MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author Harbour 
 * @date 2021-04-08 13:52
 */
object DauApp2 {
    def main(args: Array[String]): Unit = {

        // TODO 1. 获取spark streaming context
        val sparkConf: SparkConf = new SparkConf().setAppName("dauApp2").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 2. 获取kafka DStream数据流

        val topic = "mall-start"
        val groupId = "mall-start-group"
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        jedisClient.get("offset:" + topic + ":" + groupId)

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

        // TODO 3. 处理数据流


    }
}
