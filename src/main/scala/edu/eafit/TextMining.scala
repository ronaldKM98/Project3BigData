package edu.eafit

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

// Stop words.
import EnglishStopWords.englishStopWords

object TextMining {

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("BigData")

    val spark: SparkSession =
      SparkSession
        .builder
        .master("local[*]")
        .appName("Text Mining")
        .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // Read the data.
    val df: DataFrame =
      spark
        .read
        .option(key = "header", value = "true")
        .option(key = "encoding", value = "UTF-8")
        .option(key = "sep", value = ",")
        .option(key = "inferSchema", value = "true")
        .csv("src/main/resources/all-the-news/articles1.csv",
                      "src/main/resources/all-the-news/articles2.csv",
                      "src/main/resources/all-the-news/articles3.csv")

    // Clean the data.
    val newsDF: DataFrame = df.select($"id", $"title", $"content")
        .withColumn("content", removeStopWordsUdf($"content"))
        .withColumn("title", removeStopWordsUdf($"title"))

    newsDF.show()


    // Build inverted index


    spark.stop()
  }

  /**
    * Receives a String and return its clean version without stopwords nor quotation marks.
    **/
  def removeStopWords(content: String): String  = {
    content
      .toLowerCase
      .split(" ")
      .map(_.replaceAll("""[\p{Punct}]""", ""))
      .filterNot(word => englishStopWords.contains(word))
      .mkString(" ")
  }

  val removeStopWordsUdf: UserDefinedFunction = udf(removeStopWords(_))
}