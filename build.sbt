name := "testing"

version := "0.1"

scalaVersion := "2.12.11"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided",
  "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4",
  "com.ibm.stocator" % "stocator" % "1.1.3",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3")
assemblyMergeStrategy / assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}