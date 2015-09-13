package com.viettel.vtcc

import _root_.kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]) {
    val sparkConfig = new SparkConf
    Logger.getRootLogger.setLevel(Level.INFO)
    sparkConfig.setMaster("local[2]").setAppName("Spark file streaming word count")
    val sc = new SparkContext(sparkConfig)
    val sqlContext = new SQLContext(sc)

    val streamingContext = new StreamingContext(sc,Seconds(10))
    val kafkaParams = Map("metadata.broker.list"->"localhost:9092","auto.offset.reset"->"smallest")
    val topicSet  = Set("lines")


    val lineStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamingContext,kafkaParams,topicSet)

    val lines = lineStream.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    wordCounts.foreachRDD { rdd =>
      // Convert RDD to DataFrame and register it as a SQL table
      val wordCountsDataFrame = sqlContext.createDataFrame(rdd)
      wordCountsDataFrame.registerTempTable("word_counts")

      val results = sqlContext.sql("SELECT * FROM word_counts")

      results.map(t => "Name: " + t(0)).collect().foreach(println)
    }



    //wordCounts.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
