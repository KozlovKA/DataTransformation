package by.kozlov.iba

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

import scala.collection.mutable.ArrayBuffer

object DataLoad {

  def productIDDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (x <- 0 to 20000) {
      result += x
    }
    result
  }

  def productGroupDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 20000) {
      val r = scala.util.Random
      result += r.nextInt(10)
    }
    result
  }

  def yearDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 20000) {
      val r = scala.util.Random
      val k = 2015 + r.nextInt((2018 - 2015) + 1)
      result += k
    }
    result
  }

  def purchaseAmountDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 20000) {
      val r = scala.util.Random
      result += r.nextInt(100000)
    }
    result
  }

  def tableCreation(spark: SparkSession, productID: ArrayBuffer[Int], productGroup: ArrayBuffer[Int], year: ArrayBuffer[Int], jan: ArrayBuffer[Int],
                    feb: ArrayBuffer[Int], mar: ArrayBuffer[Int], apr: ArrayBuffer[Int], may: ArrayBuffer[Int], jun: ArrayBuffer[Int],
                    jul: ArrayBuffer[Int], aug: ArrayBuffer[Int], sep: ArrayBuffer[Int],
                    oct: ArrayBuffer[Int], nov: ArrayBuffer[Int], dec: ArrayBuffer[Int]):Unit = {
    for (x <- 0 to 20000) {
      val table_productID = productID.apply(x)
      val table_productGroup = productGroup.apply(x)
      val table_year = year.apply(x)
      val table_jan = jan.apply(x)
      val table_feb = feb.apply(x)
      val table_mar = mar.apply(x)
      val table_apr = apr.apply(x)
      val table_may = may.apply(x)
      val table_jun = jun.apply(x)
      val table_jul = jul.apply(x)
      val table_aug = aug.apply(x)
      val table_sep = sep.apply(x)
      val table_oct = oct.apply(x)
      val table_nov = nov.apply(x)
      val table_dec = dec.apply(x)
      spark.sql(s"INSERT INTO products_record VALUES($table_productID,$table_productGroup,$table_year,$table_jan,$table_feb,$table_mar,$table_apr,$table_may,$table_jun,$table_jul,$table_aug,$table_sep,$table_oct,$table_nov,$table_dec)")
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession
      .builder()
      .appName("ibaTask")
      .master("local[*]")
      .getOrCreate()
    val input = ss
      .read
      .format("jdbc")
      .option("username", sys.env("dbUsername"))
      .option("password", sys.env("dbPassword"))
      .option("driver", "com.ibm.db2.jcc.DB2Driver")
      .option("url", "jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50000/BLUDB:user=" + sys.env("dbUsername") + ";PWD=" + sys.env("dbPassword") + ";Security=SSL;")
      .option("dbtable", "PRT00338.products_record")
      .load()
    input.createOrReplaceTempView("products_record")
    val productID = productIDDefinition()
    val productGroup = productGroupDefinition()
    val year = yearDefinition()
    val jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec = purchaseAmountDefinition()
    tableCreation(ss, productID, productGroup, year, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)
    input.show()
  }
}
