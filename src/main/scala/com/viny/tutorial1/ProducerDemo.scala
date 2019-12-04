package com.viny.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerDemo {
  def main(args: Array[String]): Unit = {

    //Create Producer Properties
    val properties:Properties = new Properties()

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //Create Producer
    val producer:KafkaProducer[String,String] = new KafkaProducer[String,String](properties)

    //Producer Record
    val record:ProducerRecord[String,String] = new ProducerRecord[String,String]("vin_test","hello world!!")

    //Send Data.This is aync sent
    producer.send(record)

    //Wait for producer to finish sending
    producer.flush()
    producer.close()

  }

}
