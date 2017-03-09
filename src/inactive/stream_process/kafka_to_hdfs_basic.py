from __future__ import print_function
import sys
import time
from kafka import KafkaConsumer
import os
import json

def _tostring(data, ignore_dicts = False):
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

class Consumer(object):
   def __init__(self, bootstrap_servers, topic):
       """Initialize Consumer with kafka broker IP and topic."""
       self.bootstrap_servers = bootstrap_servers
       self.topic = topic
       self.hdfs_path = "/user/fleetingly/history"
       self.consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers)
       self.consumer.subscribe([topic])
       self.block_cnt = 0


   def consume_topic(self, output_dir):
       """Consumes a stream of messages from the topic.
       Args:
           output_dir: string representing the directory to store the 1MB
               before transferring to HDFS
       Returns:
           None
       """
       timestamp = time.strftime('%Y%m%d%H%M%S')
       # open file for writing
       self.temp_file_path = "%s/kafka_%s_%s.txt" % (output_dir,
                                                        self.topic,
                                                        timestamp)
       self.temp_file = open(self.temp_file_path,"w")

       messageCount = 0
       for message in self.consumer:
           incoming_message = json.loads(message.value,object_hook=_tostring)
           plat = incoming_message["pickup_latitude"];
           if plat is None:
               plat = str(0)
           plon = incoming_message["pickup_longitude"];
           if plon is None:
               plon = str(0)
           messageCount += 1
           self.temp_file.write("["+str(plat) + "," + str(plon) + "]\n")
           if messageCount % 1000 == 0:
               if self.temp_file.tell() > 1000000:
                   self.flush_to_hdfs(output_dir)

   def flush_to_hdfs(self, output_dir):
       """Flushes a file into HDFS.
       Args:
           output_dir: string representing the directory to store the 1MB
               before transferring to HDFS
       Returns:
           None
       """
       self.temp_file.close()

       timestamp = time.strftime('%Y%m%d%H%M%S')

       hadoop_fullpath = "%s/%s_%s.txt" % (self.hdfs_path,
                                              self.topic,
                                              timestamp)

       print("Block {}: Flushing 1MB file to HDFS => {}".format(str(self.block_cnt),
                                                                 hadoop_fullpath))
       self.block_cnt += 1

       # place blocked messages into history and cached folders on hdfs
       os.system("hdfs dfs -put %s %s" % (self.temp_file_path,
                                               hadoop_fullpath))

       os.remove(self.temp_file_path)

       timestamp = time.strftime('%Y%m%d%H%M%S')

       self.temp_file_path = "%s/kafka_%s_%s.txt" % (output_dir,
                                                        self.topic,
                                                        timestamp)
       self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':
   # if len(sys.argv) != 2:
   #     print("Usage: consumer-hdfs <bootstrap_servers>", file=sys.stderr)
   #     exit(-1)
   # print("\nConsuming messages...")
   # cons = Consumer(bootstrap_servers=sys.argv[1],
   #                 topic="stream_basic")
   cons = Consumer(bootstrap_servers="localhost:9092",topic="stream_basic")
   cons.consume_topic("/home/ubuntu/insight-taxi-pulse/tmp")
