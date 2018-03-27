package sparkHDFS

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.List

object SparkStreamingTest {
  
  def main(args: Array[String]) {

    // Create context with 3 second batch interval
    val sparkConf = new SparkConf().setMaster("local[*]") setAppName ("SparkStreamTest")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

  import org.apache.kafka.clients.consumer.ConsumerRecord
  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.kafka.common.serialization.IntegerDeserializer
  import org.apache.spark.streaming.kafka010._
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.kafka.clients.consumer.RangeAssignor
  
val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092",
  "zookeeper.connect" -> "localhost:2181",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "groupId_carriers",
  "partition.assignment.strategy" -> classOf[RangeAssignor],
  "auto.offset.reset" -> "earliest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val topics = Array("Carriers")
val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

 val lines = stream.map(record => (record.value.hashCode(), record.value))

 lines.foreachRDD(rdd => {
      //rdd.partitioner
      
      rdd.collect().map(_._2).foreach(println)
     
    })
    ssc.start()
    ssc.awaitTermination()
    
  }
}