package sparkTestOne

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.RangeAssignor

object StreamOne {

  def main(args: Array[String]) {

    // Create context with 3 second batch interval
    val sparkConf = new SparkConf().setMaster("local[*]") setAppName ("StreamOne")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groupId_carriers",
      "partition.assignment.strategy" -> "range",
      "auto.offset.reset" -> "earliest"
      //"enable.auto.commit" -> (false: java.lang.Boolean)
      )

    val topics = Array("Carriers")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val lines = stream.map(record => (record.value.hashCode(), record.value))
    
    var timestamp = System.currentTimeMillis() / 1000

    lines.foreachRDD(rdd => {
      //rdd.partitioner

      rdd.collect().map(_._2).foreach(println)
      //rdd.map(_._2).saveAsTextFile("/data/raw/Carriers/"+timestamp.toString()+".csv")

    })
    ssc.start()
    ssc.awaitTermination()

  }

}