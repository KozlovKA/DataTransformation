package by.kozlov.iba

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.expr

object DataTransformation {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val input = Configuration.inputDB
    val year_totally = input
      .select("product_id", "product_group", "year", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
      .withColumn("total_year", expr("jan + feb + mar + apr + may + jun + jul + aug + sep + oct + nov + dec"))
      .drop("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec").sort("product_id")
    year_totally.write.format("csv").save("cos://sparkbucket12.sparkobject123/product_data.csv")
  }
}
