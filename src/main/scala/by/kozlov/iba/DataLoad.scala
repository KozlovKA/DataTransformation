package by.kozlov.iba

import org.apache.log4j.{Level, Logger}

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

object DataLoad {

  def productIDDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (x <- 0 to 400000) {
      result += x
    }
    result
  }

  def productGroupDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 400000) {
      val r = scala.util.Random
      result += r.nextInt(10)
    }
    result
  }

  def yearDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 400000) {
      val r = scala.util.Random
      val k = 2015 + r.nextInt((2018 - 2015) + 1)
      result += k
    }
    result
  }

  def purchaseAmountDefinition(): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for (_ <- 0 to 400000) {
      val r = scala.util.Random
      result += r.nextInt(100000)
    }
    result
  }

  def tableCreation(con: Connection, productID: ArrayBuffer[Int], productGroup: ArrayBuffer[Int], year: ArrayBuffer[Int], jan: ArrayBuffer[Int],
                    feb: ArrayBuffer[Int], mar: ArrayBuffer[Int], apr: ArrayBuffer[Int], may: ArrayBuffer[Int], jun: ArrayBuffer[Int],
                    jul: ArrayBuffer[Int], aug: ArrayBuffer[Int], sep: ArrayBuffer[Int],
                    oct: ArrayBuffer[Int], nov: ArrayBuffer[Int], dec: ArrayBuffer[Int], size: Int): String = {
    val rs = con.prepareStatement("""INSERT INTO PRT00338.product_record VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    val rt = con.createStatement()
    val select = rt.executeQuery("""SELECT * FROM PRT00338.product_record """)
    for (x <- 102188 to size) {
      println(x)
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
      rs.setInt(1, table_productID)
      rs.setInt(2, table_productGroup)
      rs.setInt(3, table_year)
      rs.setInt(4, table_jan)
      rs.setInt(5, table_feb)
      rs.setInt(6, table_mar)
      rs.setInt(7, table_apr)
      rs.setInt(8, table_may)
      rs.setInt(9, table_jun)
      rs.setInt(10, table_jul)
      rs.setInt(11, table_aug)
      rs.setInt(12, table_sep)
      rs.setInt(13, table_oct)
      rs.setInt(14, table_nov)
      rs.setInt(15, table_dec)
      rs.addBatch()
      rs.executeUpdate()
    }
    var result: String = ""
    while (select.next()) {
      result = select.getString("product_id")
    }
    rs.close()
    result
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  def connectURL(): java.sql.Connection = {
    val url = "jdbc:db2://dashdb-txn-sbox-yp-dal09-14.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;"
    val username = sys.env("dbUsername")
    val password = sys.env("dbPassword")
    Class.forName("com.ibm.db2.jcc.DB2Driver")
    val connection = java.sql.DriverManager.getConnection(url, username, password)
    connection
  }

  def main(args: Array[String]): Unit = {
    val tableSize: Int = 400000
    val con: java.sql.Connection = connectURL()
    val productID = productIDDefinition()
    val productGroup = productGroupDefinition()
    val year = yearDefinition()
    val jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec = purchaseAmountDefinition()
    tableCreation(con, productID, productGroup, year, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec, tableSize)
  }
}
