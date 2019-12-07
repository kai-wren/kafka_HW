import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

object CustomClassForKeyDemo extends App {

  val props:Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "InstEmpSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[InstEmp, String](props)

  for (i <- 0 to 25) {
      for (t <- List("Office", "Landfill", "Logistics", "Warehouse")) {
        val record = new ProducerRecord[InstEmp, String]("exercise_8", InstEmp(t, i),
        "Employee #"+Integer.toString(i)+" from department "+t)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = if(exception != null) {
          exception.printStackTrace()
        } else {
          println("The serialized key size of the record we just sent is: " + metadata.serializedKeySize())
        }
      })}
  }
  producer.close()

}

case class InstEmp(InstID: String, EmpID: Int)

class InstEmpSerializer extends Serializer[InstEmp]{
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  def serialize(topic: String, data: InstEmp): Array[Byte] = {
  data match {
    case InstEmp(i, e) if i.startsWith("Office") => ("AAA"+e.toString).getBytes //Assign some codes instead of long departments name + employee ID
    case InstEmp(i, e) if i.startsWith("Landfill") => ("BBB"+e.toString).getBytes
    case InstEmp(_, e) => ("CCC"+e.toString).getBytes //default value for all departments
    }
  }

  def close(): Unit = {}
}
