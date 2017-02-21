

import java.util.Calendar
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.elasticsearch.hadoop._
import org.apache.spark.sql._

import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import utils.SparkKafkaSink

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import com.codahale.jerkson.Json
import scala.util.parsing.json.JSON

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global




/**
 * Demo of streaming k-means with Spark Streaming.
 * Reads data from one directory, and sends to  --> Kafka
 * Also sends to -> ES
 *
 */

object ClusterAnalysis {





  def main(args: Array[String]) {



    val conf = new SparkConf().setMaster("local[12]").setAppName("KMeansDemo")
       .set("es.index.auto.create", "true")
       .set("es.nodes", "ec2-34-193-153-112.compute-1.amazonaws.com")
       .set("es.port", "9200")
       .set("es.net.http.auth.user", "elastic")
       .set("es.net.http.auth.pass", "changeme")
       .set("spark.es.nodes.discovery","false")
       .set("spark.es.http.timeout","5m")

    val batchDuration = 5
    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    val brokers = ":9092"
    val topics = "usersenriched"
    val topicsSet = topics.split(",").toSet
    val sc = ssc.sparkContext



// FOR Direct to ES - needs auth
//     val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//     val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//     sc.makeRDD(Seq(numbers, airports)).saveToEs("newtest/docs")
//     val RDD = sc.esRDD("users/user")


//    FROM HDFS:
//    val trainingData = ssc.textFileStream(inputDir).map(Vectors.parse)
//    trainingData.print()

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams, topicsSet).map(_._2).repartition(64)
    messages.print()
    // val points = messages.map(x => parse(x))
    // val parsedEvents = messages.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String,Any]])
    val events = messages.map(line => line.split(",") match {
     case Array(s1,s2, s3, s4, points @ _*) => (s1,s2,s3,s4)
   })
    // parsedEvents.print()
    val output = events.map(x => Map("cluster"->x._1.toString,
      "location"-> Map("lat"->x._2.toString, "lon"->x._3.toString),
      "time_stamp"-> x._4.toString)) //"time_stamp"-> "2017-02-20T02:12:13.610"
    output.print()
//    events.print()

//  Checking Partitions
//    println("partition size")
//    events.foreachRDD { rdd =>
//      val numParts = rdd.partitions.size
//      println("[%s:%s]".format(numParts.toString,"partition size"))
//    }

//    Parsing (if text file is not already formatted correctly)
//    def trainFormed = trainingData.map(line => line.split(";") match {
//      case Array(s1,s2, points @ _*) => "[%s,%s]".format(s1,s2)
//    })

//    trainFormed.print()
//    val trainReady = trainingData.map(Vectors.parse)
//    trainReady.print()



    // val tupleData = points.map(x => (compact(render(x \ "VendorID")).toDouble,
    //   (compact(render(x \ "pickup_latitude")).toDouble,
    //     compact(render(x \ "pickup_longitude")).toDouble,
    //     compact(render(x \ "timestamp")).split(" ") match {
    //             case Array(s1,s2, points @ _*) => ("%sT%s".format(s1,s2)).replace("\"", "")  //yyyy-MM-dd'T'HH:mm:ss.SSS
    //           }
    //     )
    //   )
    // )
    // tupleData.print()




    // Send Data to Elasticsearch
    output.foreachRDD { rdd => {
      rdd.saveToEs("real-time-clusters/user")
    }
    }



    ssc.start()
    ssc.awaitTermination()
  }
}
