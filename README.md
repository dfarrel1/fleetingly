Fleetingly
===========================
### (Built Using Four Nodes on AWS)
In this project I have built and app which analyzes fleet behavior with respect to user distribution in real time.

**--add image here--**

Here are the details of how I approached this problem:

1. Data Collection: Collected NYC Taxi Cab data. Moved to S3 and streamed this into my pipeline, simulating a high input by artificially replacing timestamp in realtime.

2. Here's how my pipeline looks like:

**--add image here--**

<!--![screenshot from 2015-06-25 23 15 14](https://cloud.githubusercontent.com/assets/9309804/8371720/133ff08e-1b90-11e5-9258-b0b45afbdf7b.png)-->

I use Streaming K-means in the spark streaming environment. There are three indices are created in elasticsearch. 
One contains data about users locations. Another contains information about cars locations. A third keeps the locations of the clusters found with KMeans.

Engineering challenges : 

1. Tunning Kafka, Spark Streaming and Elasticsearch in order to update the map as quickly as possible. 

2. Producing to Kafka back from Spark Streaming in order to run MapReduce Jobs efficiently and based on cluster.