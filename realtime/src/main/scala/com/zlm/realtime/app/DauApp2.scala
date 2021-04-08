package com.zlm.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zlm.realtime.bean.DauInfo
import com.zlm.realtime.utils
import com.zlm.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

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

        // 根据topic, groupId获取redis中保存的偏移量，为了精准一次性消费
        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

        val kafkaRecordStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)

        // 保存当前kafka数据流中的偏移量信息
        var offsetRanges: Array[OffsetRange] = Array.empty

        // TODO 3. 处理数据流
        kafkaRecordStream.transform(
            // TODO : 3.1 读取并保存 kafkaRDD 中的偏移量
            (rdd: RDD[ConsumerRecord[String, String]]) => {
                // kafka的DStream中必定存在一个偏移量的数据,而kafkaRDD又是一个private的object，
                // 因此必须将其转换为父类，拿到它的属性，最后在rdd操作完成后，再更新redis内存中的偏移量
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        ).map(
            // TODO 3.2 将每个 record 中的数据封装为 json 对象
            (record: ConsumerRecord[String, String]) => {
                // 将record的值转换为 json对象
                val jsonObj: JSONObject = JSON.parseObject(record.value())
                val timestamp: Long = jsonObj.getLong("ts")

                // 提取其中的timestamp 并转换日期和小时，进行保存
                val time: Array[String] =
                    new SimpleDateFormat("yyyy-MM-dd HH mm").format(timestamp).split(" ")
                jsonObj.put("dt", time(0))
                jsonObj.put("hr", time(1))
                jsonObj.put("mi", time(2))
                jsonObj
            }
        ).mapPartitions(
            // TODO : 3.3 过滤重复数据
            (jsonIter: Iterator[JSONObject]) => {

                // 利用redis中 sadd的特性，来保证无重复元素, 其中key为日期，value为mid
                val jedisClient: Jedis = MyRedisUtil.getJedisClient
                val listBuffer = new ListBuffer[JSONObject]

                jsonIter.foreach(
                    (jsonObj: JSONObject) => {
                        val dauKey: String = "dau: " + jsonObj.getString("dt")
                        val dauValue: String = jsonObj.getJSONObject("common").getString("mid")
                        // 添加成功 返回 1  ； 已存在  返回 0
                        val exist: lang.Long = jedisClient.sadd(dauKey, dauValue)

                        // 需要给这个key设置一个过期时间
                         if (jedisClient.ttl(dauKey) < 0) jedisClient.expire(dauKey, 24*3600)
//                        jedisClient.expire(dauKey, 24*3600)

                        if (exist == 1L) listBuffer.append(jsonObj)
                    }
                )

                jedisClient.close()

                listBuffer.iterator
            }
        ).foreachRDD(
            // TODO ：3.4 批量保存，执行行动算子
            (rdd: RDD[JSONObject]) => {
                rdd.foreachPartition(
                    (iter: Iterator[JSONObject]) => {
                        // 需要将这些 json 对象，转换为 DauInfo 样例类
                        val dauList: List[DauInfo] = iter.map(
                            (jsonObj: JSONObject) => {
                                val common: JSONObject = jsonObj.getJSONObject("common")
                                DauInfo(
                                    common.getString("mid"),
                                    common.getString("uid"),
                                    common.getString("ar"),
                                    common.getString("ch"),
                                    common.getString("vc"),
                                    jsonObj.getString("dt"),
                                    jsonObj.getString("hr"),
                                    jsonObj.getString("mi"),
                                    jsonObj.getLong("ts")
                                )
                            }
                        ).toList
                        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                        MyESUtil.bulkInsert(dauList, "mall2021_dau_info_" + dt)
                    }
                )
            }
        )

        // 将此过程处理完成后的偏移量保存到redis中去
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)

        //测试输出 2
        ssc.start()
        ssc.awaitTermination()
    }
}
