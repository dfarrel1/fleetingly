

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
import scala.util.parsing.json._
// import play.api.libs.json._
import spray.json._
import DefaultJsonProtocol._ 


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

object KMeans {





  def main(args: Array[String]) {

//    For reading batches from HDFS
    val inputDir = "hdfs://ec2-34-193-153-112.compute-1.amazonaws.com:9000/user/fleetingly/history"
    val outputDir = "hdfs://ec2-34-193-153-112.compute-1.amazonaws.com:9000/user/fleetingly/models"
    val batchDuration = 5
    val numClusters = 8
    val numDimensions = 2
    val halfLife = 5
    val timeUnit = "batches"


    val conf = new SparkConf().setMaster("local[12]").setAppName("KMeansDemo")
       .set("es.index.auto.create", "true")
       .set("es.nodes", "ec2-34-193-153-112.compute-1.amazonaws.com")
       .set("es.port", "9200")
       .set("es.net.http.auth.user", "elastic")
       .set("es.net.http.auth.pass", "changeme")
       .set("spark.es.nodes.discovery","false")
       .set("spark.es.http.timeout","5m")


    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    val brokers = ":9092"
    val topics = "users"
    val topicsSet = topics.split(",").toSet
    val sc = ssc.sparkContext



// FOR Direct to ES - needs auth
//     val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//     val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//     sc.makeRDD(Seq(numbers, airports)).saveToEs("newtest/docs")
//     val RDD = sc.esRDD("users/user")

//  writeToKafka
    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", "ec2-34-193-153-112.compute-1.amazonaws.com:9092")
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }

//    TO->KAFKA -- USAGE:
//    rdd.writeToKafka(
//      producerConfig,
//      s => new ProducerRecord[String, String](topic, s)
//    )

//    FROM HDFS:
//    val trainingData = ssc.textFileStream(inputDir).map(Vectors.parse)
//    trainingData.print()

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams, topicsSet).map(_._2).repartition(64)
    val points = messages.map(x => parse(x))

    val parsedEvents = messages.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String,Any]])
    val events = parsedEvents.map(data=>"[%s,%s]".format(data("pickup_latitude").toString,data("pickup_longitude").toString))
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



    val model = new StreamingKMeans()
      .setK(numClusters)
      .setHalfLife(halfLife, timeUnit)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(events.map(Vectors.parse))
//    print(model.latestModel().clusterCenters)


    val predictions = model.predictOnValues(events.map(Vectors.parse).map(lp => (1.toDouble, lp)))

    val tupleData = points.map(x => (compact(render(x \ "VendorID")).toDouble,
      (compact(render(x \ "pickup_latitude")).toDouble,
        compact(render(x \ "pickup_longitude")).toDouble,
        compact(render(x \ "timestamp")).split(" ") match {
                case Array(s1,s2, points @ _*) => ("%sT%s".format(s1,s2)).replace("\"", "")  //yyyy-MM-dd'T'HH:mm:ss.SSS
              }
        )
      )
    )
    tupleData.print()

//    compact(render(x \ "pickup_datetime")).toDouble,
    val predFull = tupleData.join(predictions)


    val output = predFull.map(x => Map("cluster"->x._2._2,
      "location"-> Map("lat"->x._2._1._1, "lon"->x._2._1._2),
      "time_stamp"-> x._2._1._3)) //"time_stamp"-> "2017-02-20T02:12:13.610"
    output.print()


    val clustercounts = predFull.map(x => x._2._2).countByValue()
    clustercounts.print()


//     predictions.foreachRDD { rdd =>
//       val modelString = model.latestModel().clusterCenters
//         .map(c => c.toString.slice(1, c.toString.length-1)).mkString("\n")
//       val predictString = rdd.map(p => p.toString).collect().mkString("\n")
//       val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
// //      Utils.printToFile(outputDir, dateString + "-model", modelString)
// //      Utils.printToFile(outputDir, dateString + "-predictions", predictString)
//       print(modelString)
//       // print(predictString)
//     }

//    // Using Package WriteToKafka
//    predictions.foreachRDD { rdd =>
//      rdd.map(_.toString).writeToKafka(
//        producerConfig,
//        s => new ProducerRecord[String, String]("my-topic", s)
//      )
//    }

predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length-1))
      val modelMap = modelString.map(x => Map("lat"->x,
      "lon"->x))
      print(modelMap)
    }


//    // Parallelizing Kafka Producer Efficiently
    val kafkaSink = sc.broadcast(SparkKafkaSink(producerConfig))


  // val kafkaOutput = predFull.map(x => 
  //   (x._2._2,"{\"cluster\": %d, \"lat\":%f,\"lon\":%f,\"timestamp\":%s}".format(x._2._2, x._2._1._1, x._2._1._2, x._2._1._3))) // cluster, loc (lat,lon), ts
  // kafkaOutput.print()

  // val kafkaOutput = predFull.map(x => 
  //   (x._2._2,("\"{\"cluster\": %d, \"location\": {\"lat\":%f,\"lon\":%f},\"timestamp\":%s}\"".format(x._2._2, x._2._1._1, x._2._1._2, x._2._1._3)).parseJson)) // cluster, loc (lat,lon), ts
  // kafkaOutput.print()

    val kafkaOutput = predFull.map(x => 
    (x._2._2,"%s,%s,%s,%s".format(x._2._2, x._2._1._1, x._2._1._2, x._2._1._3))) // cluster, loc (lat,lon), ts
  kafkaOutput.print()

    // Seq(
    // """{"user":"helena","commits":98, "month":3, "year":2015}"""

//    send(topic: String, key: String, value: String)
    val newtopic = "usersenriched"
    kafkaOutput.foreachRDD { rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(newtopic,message._1.toString, message._2)
      }
    }



    // Send Data to Elasticsearch
    output.foreachRDD { rdd => {
      rdd.saveToEs("realtime2/user")
    }
    }



    ssc.start()
    ssc.awaitTermination()
  }
}
