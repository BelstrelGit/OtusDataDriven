package    com.belstrel.otusanalytics.consumer

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

object CustomConsumer  extends App with LazyLogging {


  override def main(args: Array[String]): Unit = {
    consumeFromKafka("wikiflow-topic")
  }
  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")


//    Thread.sleep(1200)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      println("RECORDDDD    RECORD  "   + record )
      for (data <- record.iterator) {
        println(data.value().toString)
      }
    }

//    def writeFile(filename: String, s: String): Unit = {
//      val file = new File(filename)
//      val bw = new BufferedWriter(new FileWriter(file))
//      bw.write(s)
//      bw.close()
//    }



  }
}
