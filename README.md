# Project 3 – Big Data / Spark: Text Mining for news processing

## Team Members
* Ronald Cardona Martínez - rcardo11@eafit.edu.co

* Alex Montoya Franco - amonto69@eafit.edu.co

* Camila White Romero - cwhiter@eafit.edu.co

## Dev Environment: DataBricks

## Notebook: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4563577665867992/3862672128133604/3374974215419580/latest.html

## Technical Details

### Simple Spark self-contained app.
* Build using SBT and **Java 8** 
* Don't forget to `unzip "src/main/resources/all-the-news/*.zip"`

## Methodology: CRISP-DM

## Cross Industry Standard Process for Data Mining

![](CRISP-DM.png)

## Phases:

1. Business Understanding
    * Understanding of the project goals and requirements.
    * Definition of the Data Mining problem.
2. Data Understanding
    * Acquisition of the initial dataset.
    * Exploration of the dataset.
    * Identify the quality characteristics of data.
3. Data Preparation
    * Data Choosing.
    * Data Cleaning.
4. Modeling
    * Implementation on Data Mining Tools.
5. Evaluation
    * Determine if results meets with project goals.
    * Identify the project topics that should be covered.
6. Deployment
    * Install the models on practice.

## Business Understanding

### Context: 

Text mining or text analytics are a model, techniques, algorithms and technologies sets which allow us to process text of Non-structured nature.

Text mining allow us to transform text into a structured form, so that it will be easier a serie of application such as search in text, relevance of documents, natural understanding of language (NLP), automatic translation between languages, analysis of feelings, detection of topics among many other applications.

Perhaps the simplest processing of all, be the wordcount, which is to determine the frequency of the word per document or the entire dataset.

### Problem:

### First Part: Data Preparation

The news must be pre-processed to prepare the data for the analytics, within the preparation suggestions are:

1. Remove special characters ( . , % ( ) ‘ “ ….
2. Remove stop-words
3. Remove words of longitude 1

### Second Part: An easy search tool based on the inverted index.

The inverted Index is a data structure that contains the following structure:

| Word          | News list (5 news of major frequency)    |
| ------------- |:-------------:|
| word1      | [(news1,f1), (news11,f11),(news4,f4),(news10,f10), … (news50,f50) ] |
| word2      | [(news2,f2), (news14,f14),(news1,f1),(news20,f20), … (news3,f3) ]      |
| word3      | [(news50,f50), (news1,f1),(news11,f11),(news21,f21), … (news2,f2) ]      |
| ... | ... |
| wordN| |

In the inverted index you will have the frequency of each word in the title + description.

Where for each word that is entered by keyboard in the Notebook, list in descending order by word frequency in the content "<"title">" of the news, the most relevant news. List max 5 "<"frec, news_id, title">".

frec = frequency of the word in the news "<"id">" (include title and description)
id = id of the news
title = title of the news

Enter the word (\quit to exit): house

10,17283, House Republicans Fret About Winning Their Health Care Suit
8,17295, Mar-a-Lago, the Future Winter White House and Home of the Calmer Trump
6, 17330, House Republicans, Under Fire, Back Down on Gutting Ethics Office
…


### Third Part: News clustering based on similarity.

Carry out clustering of news using one of different algorithms and models of clustering or similarity,in such a way that allows any news to identify that other news are similar.

It is proposed to use a metric / similarity function based on the intersection of the most common words for each news, it is suggested to have 10 most frequent words for news whose frequency is greater than 1.

| News          | Top 10 of words most frequent per news (without stop-words)   |
| ------------- |:-------------:|
| News1 | [(word1,f1), (word11,f11),(word4,f4),(word10,f10), … (word50,f50) ] |
| News2 | [(word2,f2), (word14,f14),(word1,f1),(word20,f20), … (word3,f3) ] |
| News3 | [(word50,f50), (word1,f1),(word11,f11),(word21,f21), … (word2,f2) ] |
| ... | ... |
| NewsM | |

Where for each news_id that you enter by keyboard in the Notebook, list in descending order of similarity the 5 news most related to said news_id.

id = id of the news
title = title of the news
List_news_id = news related list

Enter the news_id (\quit to exit): 17295

17295, Mar-a-Lago, the Future Winter White House and Home of the Calmer Trump, [17330,
17283, 15234, 14432, 13564]

## Data Understanding

## Data Preparation

### Cleaning Datasets
For this step, special characters, stopwords and words of length one were removed.

* The special characters were removed with the following regular expressions.
  ```
  [^a-z\\sA-Z]
  ```
* The stopwords were removed with the Tokenizer and StopWordsRemover libraries from Spark ML.
* The words of length one were also removed with regular expressions.
  ```
  [!-~]?\\b[\\w]\\b[!-~]?
  ```
A Dataframe was created with an id column and the title and content columns already clean. 


## Modeling

### Search in Inverted Index

### News Clustering

## Evaluation

## Deployment



