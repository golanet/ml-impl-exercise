package exercise

import java.net.URI

import org.apache.spark.sql.SparkSession

object Exercise {
  def apply(fileuri: URI)(implicit spark: SparkSession) = {

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fileuri.getPath)

  }
}
