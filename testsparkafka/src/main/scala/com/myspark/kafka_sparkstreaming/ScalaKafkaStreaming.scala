package com.myspark.kafka_sparkstreaming



import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.util.matching.Regex

object ScalaKafkaStreaming {
  private val log = Logger(LoggerFactory.getLogger(ScalaKafkaStreaming.getClass))

  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop001:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "fwmagic",
    "auto.offset.reset" -> "none",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val topicPartitions = Map[String, Int]("test" -> 1)

  private val conf = new SparkConf().setAppName("test-spark").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


  def main(args: Array[String]): Unit = {
    //  create redis pool
    InternalRedisClient.makePool("localhost")
    val ssc = createSc()
    val stream = createKafkaStream(ssc)
    Consume(stream)
    ssc.start()
    ssc.awaitTermination()
  }

  def createSc(): StreamingContext = {
    val ssc = new StreamingContext(conf, Seconds(60))
    ssc
  }

  def createKafkaStream(ssc: StreamingContext) = {
    val offsets = getOffsets
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParams, offsets)
    )
  }

  /*
  * Get last offset
  * */
  def getOffsets: Map[TopicPartition, Long] = {
    val jedis = InternalRedisClient.getResource
    //  创建可变集合
    val offsets = mutable.Map[TopicPartition, Long]()

    topicPartitions.foreach { it =>
      val topic = it._1
      val partitions = it._2

      for (partition <- 0 until partitions) {
        val topicPartitionKey = topic + ":" + partition
        var lastOffset = 0L
        val lastSavedOffset = jedis.get(topicPartitionKey)

        if (null != lastSavedOffset) {
          try {
            lastOffset = lastSavedOffset.toLong
          } catch {
            case e: Exception =>
              System.exit(1)
          }
        }
        offsets += (new TopicPartition(topic, partition) -> lastOffset)
      }
    }
    InternalRedisClient.returnResource(jedis)
    log.info("offset信息:"+ offsets.toString())
    offsets.toMap
  }

  def Consume(stream: InputDStream[ConsumerRecord[String, String]]) = {
    stream.foreachRDD { rdd =>
      log.info("来了老弟")
      // get offset infomation
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 计算offset  注意查看rdd.count() 如果过大而给定的处理时间很少则会造成spark处理不来而导致spark程序down掉
      val total = rdd.count()
      log.info("rdd.count():"+ total)
      val jedis = InternalRedisClient.getResource
      val pipeline = jedis.pipelined()

      // 会阻塞redis
      pipeline.multi()

      // update total offset
      pipeline.incrBy("testXiulian_nginx_access_totalRecords", total)

      // update offset
      offsetRanges.foreach { offsetRange =>
        val topicPartitionKey = offsetRange.topic + ":" + offsetRange.partition
        pipeline.set(topicPartitionKey, offsetRange.untilOffset + "")
      }

      rdd.collect().foreach { line =>
        println(ParseValue(line))
      }
	  
      // 执行，释放
      pipeline.exec()
      pipeline.sync()
      pipeline.close()
      InternalRedisClient.returnResource(jedis)
    }
  }

  /*
  * ParseString
  *
   */
  def ParseValue(access_text: String): LogBean = {
    // 处理业务逻辑
  }
}