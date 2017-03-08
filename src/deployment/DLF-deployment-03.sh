#!/bin/bash
# requires https://github.com/InsightDataScience/pegasus.git

#PEG_ROOT=$(dirname ${BASH_SOURCE})/../..
PEG_ROOT=~/SRC/pegasus-addons/
CLUSTER_NAME=de-ny-dene-03

peg up ${PEG_ROOT}/dene-master-03.yml &
peg up ${PEG_ROOT}/dene-worker-03.yml &

wait

peg fetch $CLUSTER_NAME

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} elasticsearch
peg install ${CLUSTER_NAME} kafka
peg install ${CLUSTER_NAME} kafka-manager
peg install ${CLUSTER_NAME} kibana

### Service Startups
###(must run zookeeper before kafka, etc)
### some starts have continuous log output
peg service ${CLUSTER_NAME} zookeeper start &
wait

peg service ${CLUSTER_NAME} kafka start &
wait

peg service ${CLUSTER_NAME} spark start &
wait

peg service ${CLUSTER_NAME} elasticsearch start &
wait

peg service ${CLUSTER_NAME} kibana start &
wait

#peg service ${CLUSTER_NAME} hadoop start &
#wait

BASEDIR=$(dirname "$0")
osascript $BASEDIR/iterm_quad_script_03.scpt

# peg service ${CLUSTER_NAME} kafka-manager start
echo echo peg service ${CLUSTER_NAME} kafka-manager start > start_kafka_manager_03.command
chmod +x start_kafka_manager_03.command; open start_kafka_manager_03.command

sudo pip install elasticsearch
sudo pip install kafka-python
sudo pip install kafka
sudo pip install pykafka
sudo pip install geohash
sudo pip install boto
sudo pip install boto3
sudo pip install smart_open

