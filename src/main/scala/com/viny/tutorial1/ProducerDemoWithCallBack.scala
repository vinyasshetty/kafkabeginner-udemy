package com.viny.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProducerDemoWithCallBack {

  def main(args: Array[String]): Unit = {

    //Create Producer Properties
    val properties:Properties = new Properties()
    val logger = LoggerFactory.getLogger("ProducerDemoWithCallBack")

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)


    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "2")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,"1000")
    properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"500")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0")
    properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"1000")
    //If message cannot be write to broker then, it put into buffer first
    //properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"0")
    //properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,"100")

    //When set offset is -1 always
    //properties.setProperty(ProducerConfig.ACKS_CONFIG, "0")


    //Create Producer
    val producer:KafkaProducer[String,String] = new KafkaProducer[String,String](properties)

    for(i <- 0 to 10){
      //Producer Record

      val record:ProducerRecord[String,String] = new ProducerRecord[String,String]("vin_test",s"hello Vinyas ${i}")

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
            println("*" * 1000)
            logger.error("Error :" + exception)
            //throw exception
          }

        }

      })

    }


    //Wait for producer to finish sending
    producer.flush()
    producer.close()

  }


}
