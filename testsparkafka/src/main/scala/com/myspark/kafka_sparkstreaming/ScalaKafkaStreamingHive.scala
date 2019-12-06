package com.myspark.kafka_sparkstreaming


import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.matching.Regex

object ScalaKafkaStreamingHive {
  private val log = Logger(LoggerFactory.getLogger(ScalaKafkaStreamingHive.getClass()))

  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "slave001:9092,slave003:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "fwmagic",
    "auto.offset.reset" -> "none",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val topicPartitions = Map[String, Int]("test" -> 1)

  private val conf = new SparkConf().setAppName("test-kafkaspark-hive").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  case class LogBean(
                      name: String,
                      age: String,
                    )

  def main(args: Array[String]): Unit = {
    //  创建redis
    InternalRedisClient.makePool()
    val ssc = createSc()
    val sparkSession = getOrCreateSparkSession()
    val stream = createKafkaStream(ssc)
    Consume(stream, sparkSession)
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

  def getOrCreateSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop001:9000/user/hive/warehouse/")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark
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

  def Consume(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession) = {
    stream.foreachRDD { rdd =>

      // 获取offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 计算offset
      val total = rdd.count()
      log.info("rdd.count()"+ total)
      val jedis = InternalRedisClient.getResource
      val pipeline = jedis.pipelined()
      log.info("pipieline:" + pipeline)
      // 会阻塞redis
      pipeline.multi()

      // update total offset
      pipeline.incrBy("testXiulian_nginx_access_totalRecords", total)

      // update offset
      offsetRanges.foreach { offsetRange =>
        val topicPartitionKey = offsetRange.topic + ":" + offsetRange.partition
        log.info("分区信息:"+ topicPartitionKey)
        pipeline.set(topicPartitionKey, offsetRange.untilOffset + "")
      }

      import sparkSession.implicits._
      import sparkSession.sql
      rdd.collect().foreach { rowinfo =>
        log.info("处理数据")
        val df = Seq(ParseValue(rowinfo.value())).toDF()
        df.createOrReplaceTempView("testtmp")
        sql("insert into default.test select `name`,`age` from testtmp").repartition(1)
        df.write.mode(SaveMode.Append).format("orc").format("hive").partitionBy("testtmp").saveAsTable("default.test")
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
  def ParseValue(rowinfo: String): LogBean = {
    \...略\

    LogBean(name, age)
  }
}
