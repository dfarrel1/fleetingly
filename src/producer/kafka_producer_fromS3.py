"""
=======================================
Simulate Kafka Producer from local file
=======================================
@author: dene_farrell
"""
# print(__doc__)
from kafka import KafkaProducer
import sys
from datetime import datetime
import time
import pandas as pd
import yaml
import sys
import signal
import random
import os.path
import boto
import boto3
import smart_open
import numpy as np
###test test test

def yaml_loader(yaml_file):
	# os.path.dirname(os.path.realpath(__file__))
	with open(yaml_file) as yml:
		config = yaml.load(yml)
	return config



def signal_handler(signal,frame):
	print("--")
	print("--- kafka producer has been halted ---")
	print("--")
	sys.exit(0)

if __name__ == '__main__':

	# path to config file
	config_source = "config/producer_fromS3_config.yml"

	# load configuration parameters
	config = yaml_loader(config_source)

	# initialize parameters
	topics = config['topics']

	time_buffer = config['time_buffer']
	# data_source = config['data_file_name']

	bucket_name = config['bucket_name']
	folder_name = config['folder_name']
	file_name = config['file_name']

	bucket = boto.connect_s3().get_bucket(bucket_name)
	key = bucket.get_key(folder_name + file_name)
	key.open()

	data_columns = config['data_column_keys']
	port = config['port']

	# create a producer object for the given ip
	producer = KafkaProducer(bootstrap_servers=[port])

	run_list = False
	### For passing list
	if run_list:
		with smart_open.smart_open(key) as fin:
			cnt = 0
			for line in fin:
				print(line)
				numbers =[line]
				# print(type(numbers))
				cnt +=1
				if (cnt>1000):
					break
				# check for stopping input
				# signal.signal(signal.SIGINT , signal_handler)

				current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

				# select a random topic
				rand = random.randint(0,len(topics)-1)
				print(topics[rand])
				# convert DataFrame to json string
				# message = numbers
				message = numbers.append(current_time)
				print(message)
				# send messages
				# producer.send(topics[rand], value = message.strip('[]').splitlines()[0])
				#print topics[rand]
				# block until all async messages are sent
				producer.flush()
				# buffer time
				time.sleep(time_buffer)

	run_df = True
	if run_df:
		### For converting to dataframe
		s3 = boto3.client('s3')
		obj = s3.get_object(Bucket=bucket_name, Key=folder_name + file_name)
		nchunk_data = 1;
		df = pd.read_csv(obj['Body'],error_bad_lines=False,sep=',',header=0, iterator=True,chunksize = nchunk_data)
		for chunk in df:
				# check for stopping input
				signal.signal(signal.SIGINT , signal_handler)
				# append current time to Pandas DataFrame
				current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
				# current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
				time_dataframe  = pd.DataFrame({'timestamp':[current_time]})
				# select a random topic
				rand = random.randint(0,len(topics)-1)
				print(topics[rand])
				# convert DataFrame to json string
				message = (chunk.join(time_dataframe)).to_json(double_precision=15,orient='records')
				print(message.strip('[]').splitlines()[0])
				# send messages
				producer.send(topics[rand], value = message.strip('[]').splitlines()[0])
				#print topics[rand]
				# block until all async messages are sent
				producer.flush()
				# buffer time
				time.sleep(time_buffer)
