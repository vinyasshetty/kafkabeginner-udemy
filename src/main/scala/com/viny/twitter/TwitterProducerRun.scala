package com.viny.twitter
import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Client, Constants, HttpHosts}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor

import scala.collection.JavaConverters._
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

class TwitterProducer(val key:String,
                      val secretKey:String,
                      val token:String,
                      val secretToken:String,
                      val msgQueue:LinkedBlockingQueue[String]){

  def setUpTwitterClient():Client = {

    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint

    val terms = List("modi", "trump").asJava

    hosebirdEndpoint.trackTerms(terms)

    val hosebirdAuth = new OAuth1(key, secretKey, token, secretToken)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")                              // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    builder.build()
  }

  def createKafkaProducer()={
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    //Safe Producer
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString)

    //High ThroughPut Producer
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32*1204).toString)
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")



    val producer = new KafkaProducer[String,String](props)
    producer
  }

  def run(client:Client, producer:KafkaProducer[String,String])={
    client.connect()
/*
    try{
      while(true){
        //val msg = Option(msgQueue.poll(1,TimeUnit.SECONDS))
        val msg = Option(msgQueue.take())
        msg.foreach(println)
      }
    }
    catch{
      case e:InterruptedException => println("Found Exception")
    }
    finally {
      client.stop()
    }*/

    while(!client.isDone()){
      val msg = Option(msgQueue.take())
      msg.foreach( m => {
        producer.send(new ProducerRecord[String,String]("twitter", null, m), new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (Option(exception) != None){
              println("Error received in Producer" + exception.getMessage)
            }
          }
        })
      })
    }
  }
}

object TwitterProducerRun {
  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("TwitterProducerRun")

    val msgQueue = new LinkedBlockingQueue[String](1000)

    val key = "tM98flv2KuISO6AQBFEUwB0Mv"
    val seceretKey = "60cwwduSGWafMv11TrAZmqGFxzzBC4VLYBTfJyqAWeNGrOc3b7"

    val token = "942015555216793601-TTr7aMcEUtNHWlpFvQsrL7xg8vUpPiG"
    val secrettoken = "xJ8VCGhUWSThE7KLeYfYwgDAdQiXJtlcs26uVUZ5MvJpE"

    val tweetProducer = new TwitterProducer(key,seceretKey,token,secrettoken,msgQueue)

    val client = tweetProducer.setUpTwitterClient()

    val producer = tweetProducer.createKafkaProducer()

    tweetProducer.run(client, producer)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.info("Shutting Down Everything!!!!!")
        client.stop()
       producer.close()
      }
    })

  }

}
