package com.viny.tutorial1

import java.time.Duration
import java.util.Properties
import scala.util.control.Breaks._

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerAssignSeek {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)

    val topicpartition = new TopicPartition("vin_test",1)

    consumer.assign(List(topicpartition).asJava)

    consumer.seek(topicpartition,10)

    var count = 0

    var donotbreak = true

    while(donotbreak){
      val records = consumer.poll(Duration.ofSeconds(1))
      breakable{
        for(record <- records.asScala){
          if(count > 4){
            donotbreak=false
            break
          }
          println(
            s""" Consumer Read:
               |Key is ${record.key()},
               |Value is ${record.value()},
               |Partition is ${record.partition()},
               |Offset is ${record.offset()}
           """.stripMargin)
          count+=1
        }
      }
    }



  }

}
