# SparkStreaming-kafka-Manual-maintenance-offset
在生产环境当中，使用SparkStreaming读取Kafka数据时，一旦Spark任务由于某些原因挂断但却没有手动维护Offset，即使及时发现也将将会导致在重启过程中的数据丢失，因此需要手动将offset进行维护来避免由offset没有保存而产生的数据丢失问题

----------------------------------------------------------------
这里提供了两个案例来使用redis进行维护offset
官方给出的手动维护代码:
http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
