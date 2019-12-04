package com.viny.tutorial1


import java.time.Duration
import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer


class ConsumerVin(val prop:Properties,
                  val consumer:KafkaConsumer[String,String],
                  val latch:CountDownLatch) extends Runnable{
  consumer.subscribe(List("vin_test").asJava)


  override def run(): Unit = {

    try{
      while(true){
        val records = consumer.poll(Duration.ofSeconds(1))
        //println("Hello")
        records.asScala.foreach(record => {
          println(
            s""" Consumer Read:
               |Key is ${record.key()},
               |Value is ${record.value()},
               |Partition is ${record.partition()},
               |Offset is ${record.offset()}
           """.stripMargin)
        })
      }
    }
    catch {
      case w:WakeupException => println("Error")
    }
    finally {
      consumer.close()
      latch.countDown()
    }

  }



}

object ConsumerDemoRunnable {
  def main(args: Array[String]): Unit = {
    val latch = new CountDownLatch(1)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_app_viny")

    val consumer = new KafkaConsumer[String,String](properties)

    //val execs = Executors.newFixedThreadPool(1)
    //execs.submit(new ConsumerVin(properties,consumer,latch))
    val t1 = new Thread(new ConsumerVin(properties,consumer,latch))
    t1.start()

    println("Waiting on latch")
    Thread.sleep(10000)
    consumer.wakeup()
    println("Wakeup called")
    latch.await()

    println("Closing main function")

  }

}
