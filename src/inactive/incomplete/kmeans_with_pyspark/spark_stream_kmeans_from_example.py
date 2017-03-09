"""
=======================================
Spark Stream Process
=======================================
Summary:
- Extract json from Kafka
- Convert json to Spark DataFrame
- Filter invalid data
- Push to Kafka

"""
# from __future__ import print_function
import json
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import Row,StringType,StructType,IntegerType,StructField
from  kafka_message_sender import KafkaMessageSender

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans

import numpy as np
import os
import sys




os.environ["PYSPARK_PYTHON"] = "python2"
os.environ['PYTHONPATH'] = ':'.join(sys.path)

## configure Kafka Producer
# path to config file
config_source = "config/producer_config.yml"
kafka_sender = KafkaMessageSender(config_source)

def quiet_logs(sc):
    # remove INFO log on terminal
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def _tostring(data, ignore_dicts = False):
    #################################
    #
    # convert items in json to string
    #
    #################################

    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ _tostring(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {_tostring(key, ignore_dicts=True): _tostring(value, ignore_dicts=True) for key, value in data.iteritems()}
    # if it's anything else, return it in its original form
    return data

def json_loader(json_file):
    with open(json_file) as jsn:
        config = json.load(jsn,object_hook=_tostring)
    return config

def debug(flag,dataframe):
    if flag == True:
        dataframe.show()
    else:
        return


def stream_to_vectors(row_rdd,sc):
    try:
        if row_rdd is None or row_rdd.isEmpty():
            print("---------rdd empty!---------")
            return

        else:
            schema = StructType([
                        StructField("pickup_longitude", StringType(), True),
                        StructField("pickup_latitude", StringType(), True)])
            # convert row RDD to
            dataframe = sqlContext.createDataFrame(row_rdd,schema)

            # need this to print queries
            dataframe.createOrReplaceTempView("pickups")
            ######################
            # filter invalid data
            ######################

            # filter invalid
            df = dataframe.na.drop(how='any',subset=['pickup_longitude','pickup_latitude'])
            df.show()


            # results = sqlContext.sql("SELECT pickup_longitude FROM pickups")
            # results.show()

            rdd = df.rdd.map(lambda data: Vectors.dense([float(c) for c in data]))
            trainingData = rdd
            trainingQueue = [trainingData]

            trainingStream = ssc.queueStream(trainingQueue)
            # We create a model with random clusters and specify the number of clusters to find
            model = StreamingKMeans(k=4, decayFactor=1.0).setRandomCenters(2, 1.0, 0)
            # Now register the streams for training and testing and start the job,
            # printing the predicted cluster assignments on new data points as they arrive.
            model.trainOn(trainingStream)
            latest = sc.broadcast(model.latestModel)
            print(latest)
            centers = model.clusterCenters()
            print("Cluster Centers: ")
            for center in centers:
                print(center)

    except:
        pass

def stream_to_dataframe(row_rdd):
    flag = True

    try:
        if row_rdd is None or row_rdd.isEmpty():
            print("---------rdd empty!---------")
            return

        else:
            schema = StructType([
                        StructField("pickup_longitude", StringType(), True),
                        StructField("pickup_latitude", StringType(), True)])
            # convert row RDD to
            dataframe = sqlContext.createDataFrame(row_rdd,schema)

            # need this to print queries
            dataframe.createOrReplaceTempView("pickups")
            ######################
            # filter invalid data
            ######################

            # filter invalid
            df = dataframe.na.drop(how='any',subset=['pickup_longitude','pickup_latitude'])
            df.show()


            # results = sqlContext.sql("SELECT pickup_longitude FROM pickups")
            # results.show()

            rdd = df.rdd.map(lambda data: Vectors.dense([float(c) for c in data]))

            trainingData = rdd
            trainingQueue = [trainingData]
            trainingStream = ssc.queueStream(trainingQueue)
            # We create a model with random clusters and specify the number of clusters to find
            model = StreamingKMeans(k=4, decayFactor=1.0).setRandomCenters(2, 1.0, 0)
            # Now register the streams for training and testing and start the job,
            # printing the predicted cluster assignments on new data points as they arrive.
            model.trainOn(trainingStream)
            latest = sc.broadcast(model.latestModel)
            print(latest)
            centers = model.clusterCenters()
            print("Cluster Centers: ")
            for center in centers:
                print(center)

    except:
        pass

def get_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return json_object


if __name__ == '__main__':

    # conf =  SparkConf()
    # conf.setAppName("Stream Direct from Kafka")
    # conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    # sc = SparkContext(conf=conf)
    # ssc = StreamingContext(sc, 2)
    # # ec2-34-193-153-112.compute-1.amazonaws.com:
    # mainStream = ssc.textFileStream("localhost:9000/user/")
    # mainStream.pprint
    def g(x):
        print(x)
    # mainStream.foreachRDD(g)
    #
    # # ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint")
    #
    # ssc.start()
    # ssc.awaitTermination()


    ## initialize Spark and set configurations
    conf = SparkConf()
    conf.setAppName("Stream Direct from Kafka")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # keep INFO logs off

    quiet_logs(sc)
    ssc = StreamingContext(sc, 2)

    # begin kafka stream
    #  topic ["cars","users"]
    kafka_stream = KafkaUtils.createDirectStream(ssc, ['stream_basic'], {"metadata.broker.list":"localhost:9092"})
    # kafka_stream.pprint()
    # DStream or kafka message

    # parsed = kafka_stream.map(lambda (key,json_stream): json.loads(json_stream,object_hook=_tostring))
    # print(parsed)
    # convert to row RDD
    # places = parsed.map(lambda x: Row(str(x['pickup_longitude']),
	# 									str(x['pickup_latitude'])))

    places = kafka_stream.map(lambda x: Row(x[1],
										x[2]))
    # def emptyQ(x):
    #     if x is None or x.isEmpty():
    #         print 'empty'
    #
    # places.foreachRDD(emptyQ)
    #
    #


    #
    #
    pvecs = places.map(lambda data: Vectors.dense([float(c) for c in data]))
    pvecs.foreachRDD(g)

    # # schemaPlaces = sqlContext.createDataFrame(places)
    # # schemaPlaces.createOrReplaceTempView("places")
    # # results = sqlContext.sql("SELECT * FROM places")
    # # df2 = results
    # # results.show()
    # # rdd = df2.rdd.map(lambda data: Vectors.dense([float(c) for c in data]))
    #
    #

    dstream = ssc.textFileStream("localhost" + ":9000/user/")
    dstream.pprint()

    dstream_points=dstream.map(lambda post: get_json(post))\
         .filter(lambda post: post != False)\
         .filter(lambda tpl: tpl[2] != '')\
         .map(lambda tpl: (tpl[1],tpl[2]))


    dstream_points.pprint()
    dstream_points.foreachRDD(g)

    trainingData=dstream_points.map(lambda tpl: [tpl[0],tpl[1]])



    # We create a model with random clusters and specify the number of clusters to find
    model = StreamingKMeans(k=4, decayFactor=1.0).setRandomCenters(2, 1.0, 0)
    # Now register the streams for training and testing and start the job,
    # printing the predicted cluster assignments on new data points as they arrive.
    model.trainOn(trainingData)

    latest = sc.broadcast(model.latestModel)
    print(latest)
    centers =  model.latestModel().clusterCenters
    print("Cluster Centers: ")
    for center in centers:
        print(center)


    clust=model.predictOnValues(testdata)
    #clust.pprint()
    #words = lines.flatMap(lambda line: line.split(" "))
    topic=clust.map(lambda x: (x[1],x[0][1]))
    #topic.pprint()
    topicAgg = topic.reduceByKey(lambda x,y: x+y)
    #wordCollect.pprint()
    topicAgg.map(lambda x: (x[0],freqcount(x[1]))).pprint()

    clust.foreachRDD(lambda time, rdd: q.put(rdd.collect()))


    # kafka_stream.pprint()
    # perform filtering and dataframe conversion
    # row_rdd.foreachRDD(stream_to_dataframe)


    # begin
    ssc.start()
    ssc.awaitTermination()
