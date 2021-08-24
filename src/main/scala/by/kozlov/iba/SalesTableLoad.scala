package by.kozlov.iba

import by.kozlov.iba.Configuration.ss
import com.opencsv.CSVWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import java.io.{BufferedWriter, FileWriter}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.io.{BufferedWriter, File, FileWriter}
import java.sql
import java.sql.{Date, Timestamp}
import java.util.{Calendar, Random}
import java.sql.Connection
import java.text.SimpleDateFormat
import scala.{+:, :+, ::}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SalesTableLoad {
  val salesTableSchema = new StructType()
    .add("date", DateType, nullable = true)
    .add("product_id", IntegerType, nullable = true)
    .add("amount", IntegerType, nullable = true)
    .add("buyer", StringType, nullable = true)
    .add("seller", StringType, nullable = true)
    .add("price", FloatType, nullable = true)
    .add("discount", IntegerType, nullable = true)
    .add("total", FloatType, nullable = true)
    .add("UPDATE_TIMESTAMP", TimestampType, nullable = true)
    .add("DEAL_ID", StringType, nullable = false)


  def main(args: Array[String]): Unit = {
    Configuration.ss
    val df = ss.read
      .format("csv")
      .option("header", "true")
      .load("file.csv")
    val writeDB = df.write
      .format("jdbc")
      .option("username", sys.env("dbUsername"))
      .option("password", sys.env("dbPassword"))
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("batchsize", "500")
      .option("url", "jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50000/BLUDB:user=" + sys.env(
        "dbUsername") + ";PWD=" + sys.env("dbPassword") + ";Security=SSL;")
      .option("dbtable", "PRT00338.sales_table")
      .mode("overwrite")
      .save()

  }

}
