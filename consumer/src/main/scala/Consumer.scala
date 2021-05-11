import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Consumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "book")
  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(List("books").asJavaCollection)
  consumer.poll(Duration.ofSeconds(1))

  val maxMessagesToReturn = 5

  private val partitions = consumer.partitionsFor("books")
    .asScala
    .map(info => new TopicPartition(info.topic(), info.partition()))
    .asJavaCollection
  consumer.seekToEnd(partitions)
  partitions.foreach(topicPartition =>
    consumer.seek(topicPartition, consumer.position(topicPartition) - maxMessagesToReturn))

  consumer
    .poll(Duration.ofSeconds(1))
    .asScala
    .foreach { r => println(s"partition: $r.partition() offset: $r.offset() value: $r.value()") }

  consumer.close()
}