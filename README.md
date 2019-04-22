# Simple Spark self-contained app.
* Build using SBT and **Java 8** 
* Don't forget to `unzip "src/main/resources/all-the-news/*.zip"`

## Cleaning Datasets
For this step, special characters, stopwords and words of length one were removed.

* The special characters were removed with regular espressions.
* The stopwords were removed with the Tokenizer and StopWordsRemover libraries from Spark ML.
* The words of length one were removed
