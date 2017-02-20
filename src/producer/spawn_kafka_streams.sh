#!/bin/bash
IP_ADDR=$1
NUM_SPAWNS=$2
SESSION=$3
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do

#    The basic producer just creates tuples, but the kafka consumer expects a dataframe
#    echo $ID
#    tmux new-window -t $ID
#    tmux send-keys -t $SESSION:$ID 'python basic_kafka_producer_gps.py '"$IP_ADDR"' '"$ID"'' C-m

    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python kafka_producer.py '"$IP_ADDR"' '"$ID"'' C-m
done
