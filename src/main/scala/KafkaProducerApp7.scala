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
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "CustomPartitioner")
    val producer = new KafkaProducer[String, String](props)

  for (i <- 0 to 50) {
      for (t <- List("a", "b")){
        val recordWithKey = new ProducerRecord[String, String]("exercise_7", t+Integer.toString(i),
        "Message from scala producer #"+Integer.toString(i))
        val recordWithoutKey = new ProducerRecord[String, String]("exercise_7", "Message from scala producer #"+Integer.toString(i))
        producer.send(recordWithoutKey, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = if(exception != null) {
          exception.printStackTrace()
          } else {
          println("The partition of the record we just sent is: " + metadata.partition())
          }
      })
      }
  }
  producer.close()

}

class CustomPartitioner extends Partitioner{

  override def configure(configs: util.Map[String, _]): Unit = {} // empty method, implementing due to errors otherwise

  override def close(): Unit = {} // empty method, implementing due to errors otherwise

  override def partition(topic: String,key: Any,keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte] , cluster: Cluster):Int = {
    if(key != null) {
      if (key.asInstanceOf[String].startsWith("a")){ //all messages starting with "a" should be sent to partition 0
        0
      } else { //messages with any other characters in the beginning will be sent to other partition
        1
      }
    } else {
      value.asInstanceOf[String].last.toInt % 2 //if key is null than determine partition as remainder of division of last value character onto 2
    }

  }
}
