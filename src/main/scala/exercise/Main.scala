package exercise

import java.net.URI

import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    val sessionConfig = SparkSession.builder().appName("exercise")

    implicit val session = sessionConfig.master("local[*]").getOrCreate()

    try {
      Exercise(new URI(args.head))
    } finally {
      session.close()
    }
  }
}
