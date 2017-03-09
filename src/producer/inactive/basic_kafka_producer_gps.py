import random
import sys
import six
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import csv
import time

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol, data_file='/home/ubuntu/fleetingly/data/test_data_30.csv'):
        with open(data_file,'rb') as f:
            reader = csv.reader(f)
            row1 = next(reader)
            print(row1)
            #nchunk_data = 1
            #data_file = pd.read_csv(data_file,error_bad_lines=False,sep=',',header=0, iterator=True,chunksize = nchunk_data)
            msg_cnt = 0
            while True:
                full_line = next(reader)
                print(full_line)
                pick_time_field = full_line[1] ## lpep_pickup_datetime
                drop_time_field = full_line[2] ## Lpep_dropoff_datetime
                pick_long_field = full_line[5] ## Pickup_longitude
                pick_lat_field = full_line[6]  ## Pickup_latitude
                if pick_lat_field is not None:
                    time_field = datetime.now().strftime("%Y%m%d %H%M%S")
                    str_fmt = "{};{};{};{}"
                    message_info = str_fmt.format(source_symbol,
                                                  pick_time_field,
                                                  pick_long_field,
                                                  pick_lat_field)
                    print(message_info)

                    # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    # time_dataframe  = pd.DataFrame({'timestamp':[current_time]})
                    # message = (chunk.join(time_dataframe)).to_json(double_precision=15,orient='records')
                    # print message.strip('[]').splitlines()[0]
                    # producer.send(topics[rand], value = message.strip('[]').splitlines()[0])

                    self.producer.send_messages('stream_basic', source_symbol, message_info)
                    msg_cnt += 1
                    time.sleep(1)

if __name__ == "__main__":
    # args = sys.argv
    # ip_addr = str(args[1])
    # partition_key = str(args[2])
    ip_addr = 'ec2-34-193-153-112.compute-1.amazonaws.com'
    partition_key = '1'
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
