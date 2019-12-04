package com.viny.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProducerDemoWithKeysCallBack {

  def main(args: Array[String]): Unit = {

    //Create Producer Properties
    val properties:Properties = new Properties()
    val logger = LoggerFactory.getLogger("ProducerDemoWithCallBack")

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //Create Producer
    val producer:KafkaProducer[String,String] = new KafkaProducer[String,String](properties)

    for(i <- 0 to 10){
      //Producer Record

      val topic  = "vin_test"
      val key = s"Id_${i}"
      val value = s"Hello Sir ${i}"

      logger.info(s"key is ${key}")

      val record:ProducerRecord[String,String] = new ProducerRecord[String,String]("vin_test",key,value)

      //Send Data.This is aync sent
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit ={
          // Executed everytime after a record is succesfully sent or exception is raised
          if( Option(exception) == None){
            logger.info(
              s"""Msg Info:
                 |Topic is ${metadata.topic()},
                 |Timestamp is ${metadata.timestamp()},
                 |Offset is ${metadata.offset()}
                 |Partition is ${metadata.partition()} """.stripMargin)
          }
          else{
            logger.error("Error :" + exception)
          }

        }

      }).get()

    }


    //Wait for producer to finish sending
    producer.flush()
    producer.close()

  }


}
