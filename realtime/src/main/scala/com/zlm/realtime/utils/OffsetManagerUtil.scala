package com.zlm.realtime.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * @author Harbour 
 * @date 2021-04-07 13:37
 */
object OffsetManagerUtil {

    def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
        val offsetKey: String = "offset:" + topic + ":" + groupId
        val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()

        for (elem <- offsetRanges) {
            offsetMap.put(elem.partition.toString, elem.untilOffset.toString)
            println("当前的偏移量为：" + elem.partition + elem.untilOffset)
        }

        if (offsetMap != null && !offsetMap.isEmpty) {
            val client: Jedis = MyRedisUtil.getJedisClient
            client.hmset(offsetKey, offsetMap)
            client.close()
        }
    }


    /**
     * 从redis中，读取指定主题，指定消费者组的消费分区与偏移量信息
     *
     * @param topic 主题
     * @param groupId 消费者组
     * @return
     */
    def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
        val client: Jedis = MyRedisUtil.getJedisClient
        val offSetKey: String = "offset:" + topic + ":" + groupId
        val utilOffsetMap: util.Map[String, String] = client.hgetAll(offSetKey)
        client.close()

        // 将数据按照 Map[TopicPartition, Long] 格式返回
        import scala.collection.JavaConverters._
        utilOffsetMap.asScala.map{
            case(partition: String, offset: String) =>
                println(topic + " " + partition + " " + offset)
                (new TopicPartition(topic, partition.toInt), offset.toLong)
        }.toMap
    }

}
