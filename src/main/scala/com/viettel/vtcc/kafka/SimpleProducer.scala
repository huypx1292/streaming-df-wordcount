package com.viettel.vtcc.kafka

import java.util.Properties

import kafka.producer.{Producer, KeyedMessage, ProducerConfig}

import scala.io.Source

/**
 * Created by HuyPX on 13/09/2015.
 */
class SimpleProducer {

  def run: Unit = {
    val topic = "lines"
    val brokers = "192.168.137.1:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val lines: Array[String] = linesFromFile;

    for (line <- lines) {
      val data = new KeyedMessage[String, String](topic, line)
      producer.send(data)
    }
    print(lines.size + "messages sent to kafka")
    producer.close()
  }


  def linesFromFile: Array[String] = {
    Source.fromFile("C:\\Users\\HuyPX\\IdeaProjects\\spark-dataframe\\src\\main\\resources\\apple.txt").mkString.split("\n")
  }
}
