from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)
dataFile = ssc.textFileStream("hdfs://ec2-34-193-153-112.compute-1.amazonaws.com:9000/user/fleetingly/history")
dataFile.pprint()
ssc.start()
ssc.awaitTermination()
