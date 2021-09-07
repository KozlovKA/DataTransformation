package by.kozlov.iba

import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

object SalesTableReadAndModify extends App {
  def newDataCheck(df2DF:DataFrame,cosChangeLogDF:DataFrame) : Boolean = {
    val answer = true
    answer
  }
  val ss = SparkSession.builder()
    .master("local[*]")
    .appName("SalesTableToCos")
    .config("spark.hadoop.fs.cos.sparkobject123.access.key", sys.env("access.key"))
    .config("spark.hadoop.fs.cos.sparkobject123.secret.key", sys.env("secret.key"))
    .config("spark.hadoop.fs.cos.sparkobject123.endpoint", sys.env("endpoint"))
    .config("spark.hadoop.fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    .config("spark.hadoop.fs.cos.service.v2.signer.type", "false")
    .config("spark.hadoop.fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    .config("fs.stocator.cos.scheme", "cos")
    .config("spark.hadoop.fs.stocator.scheme.list", "cos")
    .getOrCreate()
  val db2DF = ss.read
    .format("jdbc")
    .option("username", sys.env("dbUsername"))
    .option("password", sys.env("dbPassword"))
    .option("driver", "com.ibm.db2.jcc.DB2Driver")
    .option("url", "jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50000/BLUDB:user=" + sys.env(
      "dbUsername") + ";PWD=" + sys.env("dbPassword") + ";Security=SSL;")
    .option("dbtable", "PRT00338.sales_table")
    .load()
//  val cosChangeLogDF = ss.read.load("cos://sparkbucket12.sparkobject123/base.parquet")
  val changeLogDF = db2DF.withColumn("update_timestamp_log",current_timestamp()).show()
  db2DF.write.format("parquet").mode("ignore").save("cos://sparkbucket12.sparkobject123/base.parquet")
  db2DF.write.format("parquet").partitionBy("update_timestamp").mode("ignore").save("cos://sparkbucket12.sparkobject123/current.parquet")
//  db2DF.selectExpr("min(update_timestamp)").show()
}
