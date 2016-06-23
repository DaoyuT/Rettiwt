name := "tweet_data"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(

"org.apache.spark" %% "spark-core" % "1.3.0" % "provided",

"org.apache.spark" %% "spark-sql" % "1.3.0" % "provided",

"org.apache.spark" % "spark-streaming_2.10" % "1.3.0" % "provided",

"org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",

"com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0",

"com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",

"biz.paluch.redis" % "lettuce" % "3.4.3.Final"


)

mergeStrategy in assembly := {

  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard

  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard

  case "log4j.properties"                                  => MergeStrategy.discard

  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines

  case "reference.conf"                                    => MergeStrategy.concat

  case _                                                   => MergeStrategy.first

}

