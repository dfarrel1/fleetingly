package utils

import java.util.Properties
import spray.json._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * For publishing to Kafka from every partition of an RDD *
 *
 *
 * @param createProducer
 */
class SparkKafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  /**
   * Records assigned to partitions using the configured partitioner.
   *
   * @param topic
   * @param key
   * @param value
   */
  def send(topic: String, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }

  /**
   * Records assigned to partitions explicitly, ignoring the configured partitioner.
   *
   * @param topic
   * @param partition
   * @param key
   * @param value
   */
  def send(topic: String, partition: Int, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, partition, key, value))
  }
    
//  def send(topic: String, partition: Int, key: String, value: spray.json.JsValue): Unit = {
//    producer.send(new ProducerRecord(topic, partition, key, value))
//  }
    
}

object SparkKafkaSink {
  def apply(config: Properties): SparkKafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new SparkKafkaSink(f)
  }
}