package by.kozlov.iba

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Configuration {
  val ss = SparkSession
    .builder()
    .appName("ibaTask")
    .config("option", "option`1")
    .getOrCreate()
  val inputDB = ss.read.format(sys.env("source"))
    .option("username", ss.sparkContext.getConf.get("spark.myvariables.dbUsername"))
    .option("password", ss.sparkContext.getConf.get("spark.myvariables.dbPassword"))
    .option("driver", ss.sparkContext.getConf.get("spark.myvariables.driver"))
    .option("fetchsize","2000")
    .option("numPartitions", ss.sparkContext.getConf.get("spark.myvariables.numPartitions"))
    .option("partitionColumn", ss.sparkContext.getConf.get("spark.myvariables.PartitionColumn"))
    .option("lowerBound", ss.sparkContext.getConf.get("spark.myvariables.lowerBound"))
    .option("upperBound", ss.sparkContext.getConf.get("spark.myvariables.upperBound"))
    .option("url", ss.sparkContext.getConf.get("spark.myvariables.url"))
    .option("dbtable", ss.sparkContext.getConf.get("spark.myvariables.dbtable"))
    .load()

  def main(args: Array[String]): Unit = {
    ss.sparkContext.getConf.get("option")
  }
}
