name := "MySpark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.4.1",
  "com.couchbase.client" %% "spark-connector" % "1.0.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-yarn_2.10" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031"
)