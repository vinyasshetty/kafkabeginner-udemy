package com.viny.tutorial1

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object ConsumerDemo {
  def main(args: Array[String]): Unit = {

    val logger:Logger = LoggerFactory.getLogger("ConsumerDmmo")

    //Create Consumer Properties
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"vin_app")

    //Create a KafkaConsumer
    val consumer = new KafkaConsumer[String,String](properties)

    //Subscribe a Consumer
    consumer.subscribe(List("vin_test").asJava)

    //Read from Consumer
    while(true){
      val records:ConsumerRecords[String,String] = consumer.poll(Duration.ofSeconds(1))

      records.asScala.foreach( record => println(
        s"""Key is ${record.key()} ,
           |Value is ${record.value()},
           |Partition is ${record.partition()},
           |Offset is ${record.offset()}
         """.stripMargin))

    }



  }

}
