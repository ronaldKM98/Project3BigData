package edu.eafit

import java.nio.file.Paths

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.STRONG
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.io._

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

// Stop words.
import EnglishStopWords.englishStopWords

object TextMining {

  case class Article(id: Int, title: String, content: String)

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

  /** Main function */
  def main(args: Array[String]): Unit = {
    // Read the data.
    val df: DataFrame =
      spark
        .read
        .option(key = "header", value = "true")
        .option(key = "encoding", value = "UTF-8")
        .option(key = "sep", value = "\t")
        .option(key = "inferSchema", value = "true")
        .csv("src/main/resources/all-the-news/1.csv")
                      //"src/main/resources/all-the-news/articles2.csv",
                      //"src/main/resources/all-the-news/articles3.csv")
        .withColumn("title", lower($"title"))
        .withColumn("content", lower($"content"))
        .select($"id", $"title", $"content")

    // Build inverted index.
    val articlesRDD: RDD[Article] = df.rdd
      .map(i => Article(i.getAs[Int](0), i.getAs[String](1), i.getAs[String](2)))

     val invertedIndex: RDD[(String, RDD[(Int, Int)])] = (for {
      article <- articlesRDD
      text = article.title + article.content
      word <- text.split(" ").toList
    } yield (word, (article.id, 1)))
       .groupByKey
       .mapValues(_.toList)
       .mapValues(reduceList)
       .cache()


    // Online
    val search = "Colombia".toLowerCase //StdIn.readLine().toLowerCase

    val titles: Map[Int, String] =
      articlesRDD.collect()
        .map(article => (article.id, article.title)).toMap

    val result: (String, Array[(Int, Int)]) = invertedIndex
      .filter(_._1 == search)
      .mapValues(_.collect())
      .collect()
      .toList.head

    val table: Array[(Int, Int, String)] = for {
      i <- result._2
      title = titles(i._1)
    } yield (i._2, i._1, title)

    table.foreach(println)

    spark.stop()
  }

  def reduceList(list: List[(Int, Int)]): RDD[(Int, Int)] = {
    val rdd = sc.parallelize(list)
    rdd.groupByKey().map(
      pair => (pair._1, pair._2.sum)
    )
  }

  def reducer(k1: (Int, Int), k2: (Int, Int)): Int = k1._2 + k2._2

  /**
    * Receives a String and return its clean version without stopwords nor quotation marks.
    **/
  def removeStopWords(content: String): String  = {
    content match {
      case "" => ""
      case str => str.toLowerCase
                .split(" ")
                .map(_.replaceAll("""[\p{Punct}]""", ""))
                .filterNot(word => englishStopWords.contains(word))
                .mkString(" ")
    }
  }

  def cleanArticle(article: Article): Article = {
    val cleanTitle = article.title.toLowerCase
      .split(" ")
      .map(_.replaceAll("""[\p{Punct}]""", ""))
      .filterNot(word => englishStopWords.contains(word))
      .mkString(" ")

    val cleanContent = article.content.toLowerCase
      .split(" ")
      .map(_.replaceAll("""[\p{Punct}]""", ""))
      .filterNot(word => englishStopWords.contains(word))
      .mkString(" ")

    Article(article.id, cleanTitle, cleanContent)
  }

  val removeStopWordsUdf: UserDefinedFunction = udf(removeStopWords (_))
}