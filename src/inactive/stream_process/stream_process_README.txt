None of these files were active in the final pipeline.

basic_pyspark_consumer.py - simplest consumer possible (only for testing)
kafka_message_sender.py - simple producer (only for testing) 
kafka_to_hdfs_basic.py - kafka to hdfs (code functions, but did not use in pipeline)
spark_stream.py - takes json from kafka, filters on gps, and converts to dataframe
(code works, but not using in pipeline)
test_kafka.py - (only for testing), consumes and prints messages
testing_stream_process_kafka_with_notebook.ipynb - notebook for interactive unit testing