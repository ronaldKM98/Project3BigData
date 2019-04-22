val stop_words: Array[String] = Array("a", "about", "above", "across", "after", "afterwards", "again", "against", 
"all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", 
"amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "aren’t", 
"around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", 
"beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", 
"but", "by", "call", "can", "cannot", "can’t", "co", "con", "could", "couldn’t", "cry", "de", "describe", "detail", 
"do", "don’t", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", 
"empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", 
"fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", 
"from", "front", "full", "further", "get", "give", "go", "had", "has", "hasn’t", "have", "haven’t", "he", "hence", 
"her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", 
"however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "it’s", "itself", 
"keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", 
"mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", 
"neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", 
"nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", 
"ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", 
"seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", 
"six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", 
"system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", 
"thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", 
"three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", 
"twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", 
"whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", 
"wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", 
"within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves")


import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{Tokenizer}


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


//Tokenize title
val titleTokenizer = new Tokenizer().setInputCol("title").setOutputCol("title_tokenized")
val titleTokenized: DataFrame = titleTokenizer.transform(rawDf)
//titleTokenized.show()


//Remove stopwords from title
val titleRemover: StopWordsRemover = new StopWordsRemover()
    .setInputCol("title_tokenized")
    .setOutputCol("cleanTitle")
    .setStopWords(stop_words)

val titleDF: DataFrame = titleRemover.transform(titleTokenized)            
//titleDF.show()


//Tokenize content
val contentTokenizer = new Tokenizer().setInputCol("content").setOutputCol("content_tokenized")
val contentTokenized: DataFrame = contentTokenizer.transform(titleDF)
//contentTokenized.show()


//Remove stopwords from content
val contentRemover: StopWordsRemover = new StopWordsRemover()
  .setInputCol("content_tokenized")
  .setOutputCol("cleanContent")
  .setStopWords(stop_words)
val contentDF: DataFrame = contentRemover.transform(contentTokenized)
//contentDF.show()


//Create new dataframe with id column and the title and content columns already without stopwords.
val newsDF: DataFrame = contentDF.select($"id", $"cleanTitle", $"cleanContent")
newsDF.show()


//Function to remove special chars 
def removeSpecialChars(content: Seq[String]): String  = {
  content.filter(word => word.length > 1)
         .mkString(" ")
         .replaceAll("[^a-z\\sA-Z]", "")
         .trim.replaceAll(" +"," ") 
}
val removeSpecialCharsUdf: UserDefinedFunction = udf(removeSpecialChars _)


//Function to remove single chars
def removeSingleChars(content: String): String = {
  content.replaceAll("[!-~]?\\b[\\w]\\b[!-~]?", " ")
}
val removeSingleCharsUdf: UserDefinedFunction = udf(removeSingleChars _)


//Create new dataframe with id column and the title and content columns already clean.
val df: DataFrame = newsDF.withColumn("cleanTitle", removeSpecialCharsUdf($"cleanTitle"))
                          .withColumn("cleanContent", removeSpecialCharsUdf($"cleanContent"))
                          .withColumn("cleanTitle", removeSingleCharsUdf($"cleanTitle"))
                          .withColumn("cleanContent", removeSingleCharsUdf($"cleanContent"))
df.show()
