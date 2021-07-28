package by.kozlov.iba

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.expr

object DataTransformation {
  def dataTransformation(input: DataFrame): DataFrame = {
    val year_totally = input
      .select("product_id", "product_group", "year", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
      .withColumn("total_year", expr("jan + feb + mar + apr + may + jun + jul + aug + sep + oct + nov + dec"))
      .drop("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
    year_totally
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val input = Configuration.inputDB
    input.show(3)
    val year_totally = dataTransformation(input)
    year_totally.show(5)
    year_totally.write.format("csv").save("cos://sparkbucket12.sparkobject123/product_data1.csv")
  }
}
