name := "price_data"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"
    
resolvers += "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/"
    
resolvers += "conjars.org" at "http://conjars.org/repo"
    
libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.11" % "2.1.0",
"org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0",
"org.apache.spark" % "spark-streaming_2.11" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",
"org.elasticsearch" % "elasticsearch-spark_2.11" % "5.0.0-alpha2",
"com.github.benfradet" %% "spark-kafka-0-10-writer" % "0.2.0",
"net.debasishg" %% "redisclient" % "3.2",
"org.json4s" % "json4s-jackson_2.11" % "3.2.11",
"com.typesafe" % "config" % "1.3.1",
"com.gilt" % "jerkson_2.11" % "0.6.7",
"org.apache.kafka" % "kafka_2.11" % "0.8.0",
"org.apache.kafka" % "kafka-clients" % "0.8.2.1"
//
)
    
mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}