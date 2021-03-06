

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
 * Demo of streaming K-means with Spark Streaming.
 * Reads data from HDFS and/or Kafka, Processes KMeans and sends back to --> Kafka under new topics
 * Also sends to -> ES
 * Author: Dene Farrell
 * Date: March 8, 2017
 * Created At: Insight Data Science - Data Engineering Fellows Program
 */

object KMeans {





  def main(args: Array[String]) {

//    For reading batches from HDFS // Not using in pipeline currently
    val inputDir = "hdfs://ec2-34-193-153-112.compute-1.amazonaws.com:9000/user/fleetingly/history"
    val outputDir = "hdfs://ec2-34-193-153-112.compute-1.amazonaws.com:9000/user/fleetingly/models"

//    KMeans Cluster Parameters
    val batchDuration = 5
    val numClusters = 8
    val numDimensions = 2
    val halfLife = 5
    val timeUnit = "batches"


    val conf = new SparkConf().setMaster("local[12]").setAppName("KMeansDemo")
       .set("es.index.auto.create", "true")
       .set("es.nodes", "ec2-34-206-32-123.compute-1.amazonaws.com")
       .set("es.port", "9200")
       .set("es.net.http.auth.user", "elastic")
       .set("es.net.http.auth.pass", "changeme")
       .set("spark.es.nodes.discovery","false")
       .set("spark.es.http.timeout","5m")
        
        //ec2-34-193-153-112.compute-1.amazonaws.com //de-ny-dene-02


    val ssc = new StreamingContext(conf, Seconds(batchDuration))
    val brokers = ":9092"
    val u_topics = "users"
    val u_topicsSet = u_topics.split(",").toSet
    val c_topics = "cars"
    val c_topicsSet = c_topics.split(",").toSet
    val sc = ssc.sparkContext





//  for writeToKafka Utility // using KafkaSink Now
    val producerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", "ec2-34-206-32-123.compute-1.amazonaws.com:9092")
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


    // Create direct kafka stream with brokers and topics - uncomment partitioning to increase throughput
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)    
    val u_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams, u_topicsSet).map(_._2)//.repartition(64)
    val u_points = u_messages.map(x => parse(x))
    val u_parsedEvents = u_messages.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String,Any]])
    val u_events = u_parsedEvents.map(data=>"[%s,%s]".format(data("pickup_latitude").toString,data("pickup_longitude").toString))

   
    val c_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams, c_topicsSet).map(_._2)//.repartition(64)
    val c_points = c_messages.map(x => parse(x))
    val c_parsedEvents = c_messages.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String,Any]])
    val c_events = c_parsedEvents.map(data=>"[%s,%s]".format(data("dropoff_latitude").toString,data("dropoff_longitude").toString))




//  Filtering out bad GPS Data
    val c_events_for_filter = c_parsedEvents.map(data=>"%s,%s".format(data("dropoff_latitude").toString,data("dropoff_longitude").toString))

//  in progress -- filter function // Did not yet get this to work
    def c_checkRelevantGPS(line: String): Boolean=
      {   val splits = line.split(",")
          val lat = splits(0).toDouble
          val lon = splits(1).toDouble
          lat > 40 && lat < 42 && lon < -73 && lon > -75
      }

  val c_filt_events = c_events_for_filter.map(line => line.split(",")).
    filter(x => x(0).toDouble > 39 && x(0).toDouble < 41 &&
    x(1).toDouble < -73 && x(1).toDouble > -75)


    c_filt_events.map(line => (line(0),line(1))).print()
      




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




// Create KMeans Model
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setHalfLife(halfLife, timeUnit)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(u_events.map(Vectors.parse))
//    print(model.latestModel().clusterCenters)
    val latest = sc.broadcast(model.latestModel)


    val u_predictions = model.predictOnValues(u_events.map(Vectors.parse).map(lp => (1.toDouble, lp)))
    val c_predictions = model.predictOnValues(c_events.map(Vectors.parse).map(lp => (1.toDouble, lp)))



//   Extracting Only the Data that I'm Using
//   Converting to Tuples just to make things simpler
    val u_tupleData = u_points.map(x => (compact(render(x \ "VendorID")).toDouble,
      (compact(render(x \ "pickup_latitude")).toDouble,
        compact(render(x \ "pickup_longitude")).toDouble,
        compact(render(x \ "timestamp")).split(" ") match {
                case Array(s1,s2, points @ _*) => ("%sT%s".format(s1,s2)).replace("\"", "")  //yyyy-MM-dd'T'HH:mm:ss.SSS
              }
        )
      )
    )
    u_tupleData.print()

//  Enrich the user data with the cluster assignments
    val u_predFull = u_tupleData.join(u_predictions)

    val c_tupleData = c_points.map(x => (compact(render(x \ "VendorID")).toDouble,
      (compact(render(x \ "dropoff_latitude")).toDouble,
        compact(render(x \ "dropoff_longitude")).toDouble,
        compact(render(x \ "timestamp")).split(" ") match {
                case Array(s1,s2, points @ _*) => ("%sT%s".format(s1,s2)).replace("\"", "")  //yyyy-MM-dd'T'HH:mm:ss.SSS
              }
        )
      )
    )

    c_tupleData.print()
    val c_predFull = c_tupleData.join(c_predictions)


    val u_output = u_predFull.map(x => Map("cluster"->x._2._2,
      "location"-> Map("lat"->x._2._1._1, "lon"->x._2._1._2),
      "time_stamp"-> x._2._1._3)) //"time_stamp"-> "2017-02-20T02:12:13.610"
    u_output.print()

    val c_output = c_predFull.map(x => Map("cluster"->x._2._2,
      "location"-> Map("lat"->x._2._1._1, "lon"->x._2._1._2),
      "time_stamp"-> x._2._1._3)) //"time_stamp"-> "2017-02-20T02:12:13.610"
    c_output.print()


    val u_clustercounts = u_predFull.map(x => x._2._2).countByValue()
    u_clustercounts.print()

    val c_clustercounts = c_predFull.map(x => x._2._2).countByValue()
    c_clustercounts.print()

    val counts_full = u_clustercounts.join(c_clustercounts)
    counts_full.print()
    val ratios = counts_full.map(x => (x._1,((x._2._1.toDouble/x._2._2.toDouble).toString,x._2._1.toString,x._2._2.toString)))
    ratios.print()

    u_predictions.foreachRDD { rdd => 
      val cents = model.latestModel().clusterCenters.map(_.toArray)
      match {case Array(s1, points @ _*) => (s1.toString) }  
      val clust_keys = (0 to model.latestModel().clusterCenters.map(_.toArray).length)
      // val clusts_full = clust_keys zip cents
      println(s"First Attempt: Cluster Centers")
      cents.foreach(println)
      println(s"Size:")
      println(cents.size.toString)
    }



    // Send Clusters to Elasticsearch
    ratios.foreachRDD { rdd =>
      model.latestModel().clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }

      val keyed_clusts = model.latestModel().clusterCenters.zipWithIndex.map(_ match { case (center, idx) =>
      (idx, (center(0).toString, center(1).toString))
      })
      keyed_clusts.foreach(println)
      val par_clusts = sc.parallelize(keyed_clusts)

      val clusts_full = rdd.join(par_clusts)

      val clusts_string = clusts_full.map(p => p.toString).collect().mkString("\n")
      print(clusts_string)

      val clusts_output = clusts_full.map(x => Map("cluster"->x._1,
      "ratio"->x._2._1._1,"u_count"->x._2._1._2,"u_count"->x._2._1._3,
      "location"-> Map("lat"->x._2._2._1, "lon"->x._2._2._2)))
      clusts_output.saveToEs("clusters/cluster")
    }



// Use to write data to filesystem:
//     predictions.foreachRDD { rdd =>
//       val modelString = model.latestModel().clusterCenters
//         .map(c => c.toString.slice(1, c.toString.length-1)).mkString("\n")
//       val predictString = rdd.map(p => p.toString).collect().mkString("\n")
//       val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
// //      Utils.printToFile(outputDir, dateString + "-model", modelString)
// //      Utils.printToFile(outputDir, dateString + "-predictions", predictString)
//       print(modelString)
//       print(modelString.size)
//       // print(predictString)
//     }


//    // For Using Package WriteToKafka --- No Longer Using
//    predictions.foreachRDD { rdd =>
//      rdd.map(_.toString).writeToKafka(
//        producerConfig,
//        s => new ProducerRecord[String, String]("my-topic", s)
//      )
//    }




//    // Parallelizing Kafka Producer Efficiently
    val kafkaSink = sc.broadcast(SparkKafkaSink(producerConfig))


    val u_kafkaOutput = u_predFull.map(x => 
    (x._2._2,"%s,%s,%s,%s".format(x._2._2, x._2._1._1, x._2._1._2, x._2._1._3))) // cluster, loc (lat,lon), ts
  u_kafkaOutput.print()


//  use format:   send(topic: String, key: String, value: String)
    val newtopic = "usersenriched"
    u_kafkaOutput.foreachRDD { rdd =>
      rdd.foreach { message =>
        kafkaSink.value.send(newtopic,message._1.toString, message._2)
      }
    }



    // Send Data to Elasticsearch
    u_output.foreachRDD { rdd => {
      rdd.saveToEs("realtime2/user")
    }
    }

    c_output.foreachRDD { rdd => {
      rdd.saveToEs("realtime2/car")
    }
    }



    ssc.start()
    ssc.awaitTermination()
  }
}
