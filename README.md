# Project 3 – Big Data / Spark: Text Mining for news processing

## Team Members
* Ronald Cardona Martínez - rcardo11@eafit.edu.co

* Alex Montoya Franco - amonto69@eafit.edu.co

* Camila White Romero - cwhiter@eafit.edu.co

## Dev Environment: DataBricks

## Notebook: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4563577665867992/3862672128133604/3374974215419580/latest.html

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

## Technical Details

### Simple Spark self-contained app.
* Build using SBT and **Java 8** 
* Don't forget to `unzip "src/main/resources/all-the-news/*.zip"`


## Cleaning Datasets
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
