package com.viny.tutorial1

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object ConsumerRebalance {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "rebalance_test")
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200")

    val cons = new KafkaConsumer[String,String](props)

    cons.subscribe(List("vin_test").asJava)

    while(true){
      val records = cons.poll(Duration.ofMillis(100))
      println(s""" *****Record Count ${records.count()} ********""")

      for(record <- records.asScala){
        println(
          s"""
             |Partition is ${record.partition()},
             |Offset is ${record.offset()}
           """.stripMargin)
        Thread.sleep(500)
      }

      cons.commitSync()
    }


  }

}
