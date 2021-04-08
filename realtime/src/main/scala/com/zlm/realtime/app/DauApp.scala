package com.zlm.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zlm.realtime.bean.DauInfo
import com.zlm.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author Harbour 
 * @date 2021-04-02 14:00
 */
object DauApp {

    def main(args: Array[String]): Unit = {

        // 第一步，应该先获取SSC
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Dau_App")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // 第二步，调用工具类，拿到kafka stream
        val topic = "mall-start"
        val groupId = "mall_dau_bak"

        // 获取offset，从指定的offset开始读取数据
        val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

        var recordStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetMap != null && offsetMap.nonEmpty) { // 第一次获取offset
            recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
        } else { // 之前在redis已经保存过数据了
            recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
        }

        // 从kafka的RDD中，读取数据的偏移量, 后续数据操作完以后，将偏移量重新更新到redis中
        var offsetRanges: Array[OffsetRange] = Array.empty
        val kafkaRecordStream: DStream[ConsumerRecord[String, String]] = recordStream.transform(
            (rdd: RDD[ConsumerRecord[String, String]]) => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        val jsonObjDStream: DStream[JSONObject] = kafkaRecordStream.map { record: ConsumerRecord[String, String] =>
            //获取启动日志
            val jsonStr: String = record.value()
            //将启动日志转换为 json 对象
            val jsonObj: JSONObject = JSON.parseObject(jsonStr)
            //获取时间戳 毫秒数
            val ts: java.lang.Long = jsonObj.getLong("ts")

            //获取字符串	日期 小时
            val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
            //对字符串日期和小时进行分割，分割后放到 json 对象中，方便后续处理
            val dateHour: Array[String] = dateHourString.split(" ")
            jsonObj.put("dt",dateHour(0))
            jsonObj.put("hr",dateHour(1))
            jsonObj
        }

        val filterDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions(
            (jsonObjIter: Iterator[JSONObject]) => {
                val jedisClient: Jedis = MyRedisUtil.getJedisClient
                val listBuffer = new ListBuffer[JSONObject]()

                for (iter <- jsonObjIter) {
                    val dauKey: String = "dau:" + iter.getString("dt")
                    val dauValue: String = iter.getJSONObject("common").getString("mid")
                    val isFirst: lang.Long = jedisClient.sadd(dauKey, dauValue)

                    // 过期时间如果小于0，则未设置过过期时间
                    if (jedisClient.ttl(dauKey) < 0) jedisClient.expire(dauKey, 24 * 3600)

                    if (isFirst == 1L) listBuffer.append(iter)
                }
                jedisClient.close()

                listBuffer.iterator
            }
        )

        filterDStream.foreachRDD(
            (rdd: RDD[JSONObject]) => {
                rdd.foreachPartition( // 以分区为单位对 RDD 中的数据进行处理，方便批量插入
                    (jsonIter: Iterator[JSONObject]) => {
                        val dauList: List[DauInfo] = jsonIter.map {
                            (jsonObj: JSONObject) => {
                                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                                DauInfo(
                                    commonJsonObj.getString("mid"),
                                    commonJsonObj.getString("uid"),
                                    commonJsonObj.getString("ar"),
                                    commonJsonObj.getString("ch"),
                                    commonJsonObj.getString("vc"),
                                    jsonObj.getString("dt"),
                                    jsonObj.getString("hr"),
                                    "00", // 分钟未转换
                                    jsonObj.getLong("ts")
                                )
                            }
                        }.toList
                        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                        MyESUtil.bulkInsert(dauList, "mall2021_dau_info_" + dt)
                    }
                )
            }
        )

        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)

        //测试输出 2
        ssc.start()
        ssc.awaitTermination()
    }
}


