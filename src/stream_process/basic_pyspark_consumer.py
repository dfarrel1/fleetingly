from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

sc = SparkContext(appName="basic stream example")
ssc = StreamingContext(sc, 1)
kafkaStream = KafkaUtils.createDirectStream(ssc, ["users","cars"], {"metadata.broker.list": "ec2-34-198-103-9.compute-1.amazonaws.com:9092"})

kafkaStream.pprint()

ssc.start()
ssc.awaitTermination()
