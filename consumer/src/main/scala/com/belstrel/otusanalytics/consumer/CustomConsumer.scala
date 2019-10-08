package    com.belstrel.otusanalytics.consumer

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object CustomConsumer  extends App with LazyLogging {


  override def main(args: Array[String]): Unit = {
    consumeFromKafka("stock-topic")

  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    Thread.sleep(10100)
//    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
//
//    consumer.subscribe(util.Arrays.asList(topic))
//    while (true) {
//      val record = consumer.poll(1000).asScala
//      for (data <- record.iterator)
//        println(data.value())
//    }
//    consumer.close()

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))

//    var list = scala.collection.mutable.ArraySeq[String]
//    sudo  docker exec  -i -t 8e3ed337ad0a /bin/bash
    //  refactor to csv!!!
    val file = new File("/stage/model/prices.txt" )
    file.setExecutable(true)
    file.setWritable(true)
    file.setReadable(true)
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        writeFile( file ,  data.value())
      }
    }
    consumer.close()
      //refactor to use csv
    def writeFile(file: File, line: String): Unit = {
      val bw = new BufferedWriter(new FileWriter(file, true))
        bw.write(line)
       bw.write("\n")
      bw.close()
    }




  }
}

//val props:Properties = new Properties()
//props.put("group.id", "test")
//props.put("bootstrap.servers","localhost:9092")
//props.put("key.deserializer",
//"org.apache.kafka.common.serialization.StringDeserializer")
//Props.put("value.deserializer",
//"org.apache.kafka.common.serialization.StringDeserializer")
//props.put("enable.auto.commit", "true")
//props.put("auto.commit.interval.ms", "1000")
//val consumer = new KafkaConsumer(props)
//val topics = List("topic_text")
//try {
//  consumer.subscribe(topics.asJava)
//  while (true) {
//  val records = consumer.poll(10)
//  for (record <- records.asScala) {
//  println("Topic: " + record.topic() +
//  ",Key: " + record.key() +
//  ",Value: " + record.value() +
//  ", Offset: " + record.offset() +
//  ", Partition: " + record.partition())
//}
//}
//}catch{
//  case e:Exception => e.printStackTrace()
//}finally {
//  consumer.close()
//}
