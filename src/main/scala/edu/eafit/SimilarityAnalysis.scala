package edu.eafit

// Apache Spark.
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType}

object SimilarityAnalysis {

  case class Article(id: Int, title: String, content: String) extends Serializable

  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("BigData")

  @transient val spark: SparkSession =
    SparkSession
      .builder
      .master("local[*]")
      .appName("Text Mining")
      .getOrCreate()

  @transient val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    // Read the data.

    val df: DataFrame = spark
      .read
      .option(key = "header", value = "true")
      .option(key = "encoding", value = "UTF-8")
      .option(key = "sep", value = ",")
      .option(key = "inferSchema", value = "true")
      .csv("src/main/resources/all-the-news/articles3.csv")
      /**"src/main/resources/all-the-news/articles2.csv",
        "src/main/resources/all-the-news/articles3.csv")*/
      .withColumn("title", lower($"title"))
      .withColumn("content", lower($"content"))
      .select($"id".cast(IntegerType).as("id"), $"title", $"content")

    // Build inverted index.
    val articlesRDD: RDD[Article] = df.rdd
      .map(i => Article(i.getAs[Int](0), i.getAs[String](1), i.getAs[String](2)))


    // Clustering
    // Group news by similarity
    val newsIndex: RDD[(Int, List[(String, Int)])] = (for {
      article <- articlesRDD
      text = article.title + article.content
      word <- text.split(" ")
    } yield (article.id, (word, 1)))
      .groupByKey()
      .mapPartitions{
        _.map {
          case(id, list) => (id, list.groupBy(_._1).map(
            pair => (pair._1, pair._2.map(
              _._2).sum
            )
          ).toList
            .sortWith((k1, k2) => k1._2 > k2._2)
            .take(10))
        }
      }.cache() // Save RDD in memory.

    val titles: Map[Int, String] =
      articlesRDD
        .map(article => (article.id, article.title))
        .collect()
        .toMap

    // Change the news id to search for.
    val news_id: Int = 167499

    if (titles.getOrElse(news_id, 0) == 0) {
      print("news not found")
    } else {
      // TO DO: Check if the given news_id is in the data set.

      val inNews: (Int, List[(String, Int)]) = newsIndex.filter(_._1 == news_id).first
      val newsRDD: RDD[(Int, List[(String, Int)])] = newsIndex.filter(_._1 != news_id)

      val simil: DataFrame = {
        newsRDD.map(x => sim(inNews, x))
          .toDF
          .select($"_1".alias("id"), $"_2".alias("distance"), $"_3".alias("n_words"))
      }

      val sorted: Array[(Int, String)] = simil.sort(desc("n_words"), desc("distance"))
        .select($"id")
        .take(5)
        .map(_.getInt(0))
        .map(id => (id, titles(id)))

      print(news_id, titles(news_id), sorted.mkString(" "))
    }

    spark.stop()
  }

  def reduceList(list: List[(Int, Int)]): List[(Int, Int)] = {
    list.groupBy(_._1).map(
      //id, list(f)
      pair => (pair._1, pair._2.map(_._2).sum)
    ).toList
  }

  /**
    * Similarity function to compare two given articles
    * */
  def sim(i: (Int, List[(String, Int)]), j: (Int, List[(String, Int)])): (Int, Int, Int) = {
    val intersect: List[String] = i._2.map(_._1).intersect(j._2.map(_._1))
    val total: List[(String, Int)] = i._2 union j._2

    val distance: Int = total.filter(pair => intersect.contains(pair._1)) match {
      case Nil => 0
      case x => x.reduce((elem1, elem2) => (
        elem1._1, elem1._2 + elem2._2))._2 // How to sum consecutive elements.
    }

    (j._1, distance, intersect.length)
  }
}