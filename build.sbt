name := "ibaTask"

version := "0.1"

scalaVersion := "2.12.11"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4",
  "com.ibm.stocator" % "stocator" % "1.1.3",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.scalamock" %% "scalamock" % "5.1.0" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}