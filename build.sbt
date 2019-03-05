name := "Kafka-with-Akka-Streams"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "1.0.0",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.kafka" %% "kafka" % "1.0.0",
  "com.googlecode.json-simple" % "json-simple" % "1.1",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0"
)
