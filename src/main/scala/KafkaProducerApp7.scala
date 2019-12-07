import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.Cluster
import java.util

object KafkaProducerApp7 extends App{

    val props:Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner)
    val producer = new KafkaProducer[String, String](props)

  for (i <- 0 to 100) {
    val record = new ProducerRecord[String, String]("exercise_6", Integer.toString(i),
      "Message from scala producer #"+Integer.toString(i))
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = if(exception != null) {
        exception.printStackTrace();
        } else {
        println("The offset of the record we just sent is: " + metadata.offset());
        }
    })
  }
  producer.close()

  class CustomPartitioner extends Partitioner{

    override def configure(configs: util.Map[String, _]): Unit = {}

    override def close(): Unit = {}

    override def partition(topic: String,key: Any,keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte] , cluster: Cluster):Int = {
    1
    }
  }

  }

