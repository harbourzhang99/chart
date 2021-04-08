package com.zlm.realtime.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * @author Harbour 
 * @date 2021-04-02 12:08
 */
object MyKafkaUtil {

    private val properties: Properties = MyPropertiesUtil.load("config.properties")
    val broker_list: String = properties.getProperty("kafka.broker.list")

    var kafkaParam = collection.mutable.Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> "mall-group",
        // 自动重置偏移量
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    def getKafkaStream(topic: String,
                       ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
        getKafkaStream(topic: String, ssc: StreamingContext, null, null)
    }

    def getKafkaStream(topic: String,
                       ssc: StreamingContext,
                       groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        getKafkaStream(topic: String, ssc: StreamingContext, null, groupId)
    }

    /**
     * 从指定偏移量开始从kafka拿数据
     * @param topic 主题
     * @param ssc 数据流
     * @param offsetMap 每个主题与分区，对应的消费者组的偏移量
     * @param groupId 消费者组
     * @return kafkaDStream
     */
    def getKafkaStream(topic: String,
                       ssc: StreamingContext,
                       offsetMap: Map[TopicPartition, Long],
                       groupId: String
                      ): InputDStream[ConsumerRecord[String, String]] = {
        var consumerStrategy: ConsumerStrategy[String, String] = null
        if (groupId != null) kafkaParam("group.id") = groupId
        if (offsetMap == null || offsetMap.isEmpty) {
            consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
        } else {
            consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsetMap)
        }
        KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
    }

}
