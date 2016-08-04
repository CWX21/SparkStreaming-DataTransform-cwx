package com.bodao.modeling.product

import scala.language.implicitConversions
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd._

object ProductModeling {
  private val sparkMaster = "Core1"
  private val sparkMasterPort = "7077"
  private val zkQuorum = "Core1:2181,Core2:2181,Core3:2181"
  private val group = "test1"
  private val topics = "items"
  private val numThreads = "1"
  private val brokers = "Kafka1:9092,Kafka1:9093,Kafka2:9094"

  implicit def productInfoToMap(product: ProductInfos): Map[String, String] = {
    Map("pid" -> product.pid, "name" -> product.name, "price" -> product.price, "sales" -> product.sales)
  }

  def main(args: Array[String]) {
    implicit val hbConfig = HbRddConfig()

//    ProductTableManager.createTable(force = false)
    println("created table: KafkaToHbase, family is: KafkaToHbaseProduct .....")

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster(s"spark://$sparkMaster:$sparkMasterPort")
      .setJars(List("./out/artifacts/product_modeling_jar/modeling.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val productLine = messages.map(_._2)  //获取数据
    val productInfosData = productLine map { productInfoStr =>
        ProductInfosExtractor.extractProductInfos(productInfoStr)
      } filter(_.isDefined) map {_.get}

    /**
      * 将提取的内容组成hbrdd库需要的格式
      * rdd(rowid, Map(column, value))
      */
    val dataToHbase = productInfosData map { productInfos =>
      productInfos.pid -> productInfoToMap(productInfos)
    }

    dataToHbase.foreachRDD(rdd => rdd.put2Hbase("KafkaToHbaseProduct", "ProductInfos"))

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
