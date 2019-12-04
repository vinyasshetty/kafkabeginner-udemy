package com.viny.elasticsearch

import java.time.Duration
import java.util.Properties

import com.google.gson.JsonParser

import scala.collection.JavaConverters._
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

class ElasticConsumer{

  def createClient: RestHighLevelClient = {

    //https://cwfikhcqkb:5vs3i7na49@viny-demo-8382677439.us-west-2.bonsaisearch.net:443

    val hostname = ""
    // localhost or bonsai url
    val username = ""
    // needed only for bonsai
    val password = ""
    // credentials provider help supply username and password
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))
    val builder = RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
    })
    val client = new RestHighLevelClient(builder)
    client
  }

  def createConsumer()={
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "elastic")
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20")

    val consumer = new KafkaConsumer[String,String](props)
    consumer

  }

  def extractIdStr(msg:String):String = {

    val jObject = JsonParser.parseString(msg).getAsJsonObject
    //usually need to handle instances when parse might fail and return null

    jObject.get("id_str").getAsString

  }

}

object ElasticConsumerMain {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("ElasticConsumer")

    val e = new ElasticConsumer()
    val client = e.createClient

    val consumer = e.createConsumer()
    consumer.subscribe(List("twitter").asJava)

    val bulkrequest = new BulkRequest()

    while(true){
      val records = consumer.poll(Duration.ofSeconds(1))

      val cnt = records.count()

      logger.info(s"Received ${cnt} records")

      for( record <- records.asScala){
        val msg = record.value()

        //Generic Kafka Id
        //val id = s"""${record.topic()}_${record.partition()}_${record.offset()}"""

        //ELse from the msg extract a value which will be unqiue always.Like twitter feeds "id_str"
        try{
          val id = e.extractIdStr(msg)

          //val ind = new IndexRequest("twitter", "tweets", id).source(msg, XContentType.JSON)
          val ind = new IndexRequest("twitter").id(id).source(msg,XContentType.JSON)

          bulkrequest.add(ind)
          //val ind_resp = client.index(ind, RequestOptions.DEFAULT)
          //logger.info(s"Index id is ${ind_resp.getId}")
          //logger.info(s"""Id is ${id}""")

          //Thread.sleep(1000)
        }
        catch {
          case e:Exception => logger.info(s"Found a exception for record ${record.value()}" )
        }



      }
      if (cnt>0){
        val bulresp =  client.bulk(bulkrequest,RequestOptions.DEFAULT)
        logger.info("Committing Now")
        consumer.commitSync()
        logger.info("Message Committed !!!!!")
      }
      else{
        logger.info("Found no records to consume")
      }



    }

    client.close()


  }

}
