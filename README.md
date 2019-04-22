# Simple Spark self-contained app.
* Build using SBT and **Java 8** 
* Don't forget to `unzip "src/main/resources/all-the-news/*.zip"`

## Team Members
* Ronald Cardona Martínez - rcardo11@eafit.edu.co

* Alex Montoya Franco - amonto69@eafit.edu.co

* Camila White Romero - cwhiter@eafit.edu.co


## Cleaning Datasets
For this step, special characters, stopwords and words of length one were removed.

* The special characters were removed with regular expressions.
* The stopwords were removed with the Tokenizer and StopWordsRemover libraries from Spark ML.
* The words of length one were also removed with regular expressions.

A Dataframe was created with an id column and the title and content columns already clean. 
