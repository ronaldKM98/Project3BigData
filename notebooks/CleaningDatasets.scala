// Databricks notebook source
val stop_words: Array[String] = Array("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "aren’t", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "can’t", "co", "con", "could", "couldn’t", "cry", "de", "describe", "detail", "do", "don’t", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasn’t", "have", "haven’t", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "it’s", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves")

// COMMAND ----------

import java.nio.file.Paths

import org.apache.spark.sql.functions._

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.STRONG
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.ml.feature.{Tokenizer}

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

// COMMAND ----------

// Read the data.
val rawDf: DataFrame =
  spark
    .read
    .option(key = "header", value = "true")
    .option(key = "encoding", value = "UTF-8")
    .option(key = "sep", value = ",")
    .option(key = "inferSchema", value = "true")
    .csv("dbfs:///FileStore/tables/all-the-news/*.csv")
    .select($"id", $"title", $"content")
    .withColumn("title", lower($"title"))
    .withColumn("content", lower($"content"))
//df.show()

//val rdd: RDD[Article] = df.select($"id", $"title", $"content")
 // .rdd
  //.map(i => Article(i.getAs[String](0), i.getAs[String](1), i.getAs[String](2)))

//val clean: RDD[Article] = rdd.map(cleanArticle)

// COMMAND ----------

//Tokenize title
val titleTokenizer = new Tokenizer().setInputCol("title").setOutputCol("title_tokenized")
val titleTokenized: DataFrame = titleTokenizer.transform(rawDf)

//titleTokenized.show()

// COMMAND ----------

//Remove stopwords
val titleRemover: StopWordsRemover = new StopWordsRemover()
    .setInputCol("title_tokenized")
    .setOutputCol("cleanTitle")
    .setStopWords(stop_words)

val titleDF: DataFrame = titleRemover.transform(titleTokenized)
               
//titleDF.show()

// COMMAND ----------

//Tokenize content
val contentTokenizer = new Tokenizer().setInputCol("content").setOutputCol("content_tokenized")

val contentTokenized: DataFrame = contentTokenizer.transform(titleDF)

//contentTokenized.show()

// COMMAND ----------

val contentRemover: StopWordsRemover = new StopWordsRemover()
  .setInputCol("content_tokenized")
  .setOutputCol("cleanContent")
  .setStopWords(stop_words)

val contentDF: DataFrame = contentRemover.transform(contentTokenized)

//contentDF.show()

// COMMAND ----------

val newsDF: DataFrame = contentDF.select($"id", $"cleanTitle", $"cleanContent")
newsDF.show()

// COMMAND ----------

def removeSpecialChars(content: Seq[String]): String  = {
  content.filter(word => word.length > 1)
         .mkString(" ")
         .replaceAll("[^a-z\\sA-Z]", "")
         .trim.replaceAll(" +"," ") 
}

val removeSpecialCharsUdf: UserDefinedFunction = udf(removeSpecialChars _)

// COMMAND ----------

def removeSingleChars(content: String): String = {
  content.replaceAll("[!-~]?\\b[\\w]\\b[!-~]?", " ")
}

val removeSingleCharsUdf: UserDefinedFunction = udf(removeSingleChars _)

// COMMAND ----------

val df: DataFrame = newsDF.withColumn("cleanTitle", removeSpecialCharsUdf($"cleanTitle"))
                          .withColumn("cleanContent", removeSpecialCharsUdf($"cleanContent"))
                          .withColumn("cleanTitle", removeSingleCharsUdf($"cleanTitle"))
                          .withColumn("cleanContent", removeSingleCharsUdf($"cleanContent"))
df.show()

// COMMAND ----------


