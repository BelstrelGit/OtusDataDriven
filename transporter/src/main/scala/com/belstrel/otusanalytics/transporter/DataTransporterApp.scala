package com.belstrel.otusanalytics.transporter

import java.util.Properties

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
//import akka.stream.scaladsl.{RestartSource, Source}
import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source

import scala.concurrent.duration._

object DataTransporterApp extends App with StrictLogging {
  logger.info("Initializing Producer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(10000)

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._
  import system.dispatcher

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")

  val producer = new KafkaProducer[Nothing, String](props)
  producer.flush()
  logger.info("Kafka producer initialized")



//  try {       logMessages.close()   сделать закрытие ресурса
  val logMessages = Source.
    fromFile("/transporter/stage/securities.csv").
    getLines.
    toList

  logMessages.foreach(message => {
    val record = new ProducerRecord("stock-topic",  message)
    producer.send(record)
    logger.info("STOCKS SEND TO KAFKA========" +record)
  })
      producer.close()




//  var msgCounter = 0
//  val restartSource = RestartSource.withBackoff(
//    minBackoff = 3.seconds,
//    maxBackoff = 10.seconds,
//    randomFactor = 0.2
//  ) { () =>
//    Source.fromFutureSource {
//      Http().singleRequest(Get("https://stream.wikimedia.org/v2/stream/recentchange"))
//        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
//    }
//  }
//
//  restartSource.runForeach(elem => {
//    msgCounter += 1
//
//    val data = new ProducerRecord[String, String]("wikiflow-topic", elem.data)
//
//    producer.send(data)
//
//    if (msgCounter % 100 == 0) {
//      logger.info(s"New messages came, total: $msgCounter messages")
//    }
//
//  })
}

//try {
//  for (i <- 0 to 15) {
//  val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
//  val metadata = producer.send(record)
//  printf(s"sent record(key=%s value=%s) " +
//  "meta(partition=%d, offset=%d)\n",
//  record.key(), record.value(),
//  metadata.get().partition(),
//  metadata.get().offset())
//}
//}catch{
//  case e:Exception => e.printStackTrace()
//}finally {
//  producer.close()
//}
//}
