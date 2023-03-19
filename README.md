## Data Engineering Bootcamp Final Project: An Analysis of 10 years of the S&P500 Index
### Jeff Wagg - March, 2023

The Standard and Poor's 500 (S&P500) stock index tracks the performance of 500 large US companies. It was founded in the late 1950s and can be a good indicator of the economic health of the nation. After each trading day, the price of the index fund is updated and it would therefore be useful to have an automatic means of calculating the typical gains achieved on a daily basis. To achieve this, we create a pipeline that uses historical price data (opening and closing prices) from the S&P500, and processes these data in an Extract Load and Transform (ELT) pipeline. The pipeline is capable of calculating the daily gains of the index, and updates the tables in the Data Warehouse (BigQuery) using batch processing. 

#### Data

We extract data from Yahoo Finance which contains the historical S&P500 values over a user-specified timeframe (search for symbol '^GSPC'). The file we have created for our analysis is: 

SP500_10yr.csv: Daily opening and closing prices along with the trading volume for the S&P500. The data cover the time interval from March, 2013 until March, 2023. 

The file can be created by scraping the data found here: https://finance.yahoo.com/quote/%5EGSPC/history/

#### Data Pipeline

For the pipeline itself, we use a combination of Python, Prefect and DBT to upload the data to a data lake and then a data warehouse in the Google Cloud Platform (GCP). The processing is done in batches so that new values can be uploaded after each trading day. The data are uploaded to the Google Coud Storage (GCS) Bucket data lake before being transported to a data warehouse (Bigquery) where they can then transformed with our DBT model. 

#### Dashboard

The tranformed data are plotted in a dashboard using Google Date Studio.  

#### Processing Steps

- To begin, ten years of S&P500 index data are copied from the Yahoo Finance website (see above). 

- We then create a project in GCP and download the json credentials file so that we can read and write to the data lake and data warehouse. 

- Following this, a prefect account should be created and the requirements file is copied from here: https://github.com/discdiver/prefect-zoomcamp/blob/main/requirements.txt .

- A useer should then run: 
```
pip install -r requirements.txt
```
followed by:
```
prefect orion start
```
in a conda environment on the Unix shell command line.

- We have written a prefect flow pipeline script in Python to upload the 'SP500_10yr.csv' to the data lake. This can be run by executing
```
python3 etl_web_to_gcs_jfw.py
```
to upload a parquet file version of the data to GCS (make sure that the json file is in the working dir). 

- Now run a second Prefect flow Python script called: 
```
python3 etl_gcs_to_bq_jfw.py
```
to copy the data from the data lake to the BigQuery data warehouse. 

- Next, we create a new dbt project with BigQuery as the data warehosue. In this case I called it 'stock_analysis_jfw'. One needs to upload the json key file to connect to BigQuery.

- Once DBT project is inintialized, click on 'Develop'.

- To write a model for transforming the data, create a branch of the main repository and then in the models directory we create a new file called 'sp500_newtable.sql'. In this case, the model updates the 'mythic-byway-375404.stocks.sp500' table in BigQuery by adding a new column 'gain', which is the percentage daily gain for each stock on a given day. A new table is then written to the Dataset called: 'sp500_newtable'. 

- Now, once the BigQuery table is created, we want to be able to update this with the latest S&P500 data each day. That is where the batch job comes in. A python script called 'scrape_yahoo_finance.py' uses the 'yfinance' package to download the latest market data from Yahoo Finance. It then runs a Prefect flow to append the latest data to the 'sp500' table in BigQuery. We crate a cron job so that this will run Tuesday through Saturday at 4:00 UTC after the markets from the previous days have closed. 

```
crontab -e
0 4 * * 2-6 /usr/bin/python3 /Users/j.wagg/DataScience/DataEngineeringBootcamp2023/DE-Zoomcamp-Homework/src/FinalProject/scrape_yahoo_finance.py
```

- The Transform of the latest data is done in DBT. We create an 'Environment' called 'DailyEnvironment' and set the Dataset to the project name we have chosen to host our DBT model (in this case, 'stock_analysis_jfw'). We then create a new job which we call 'DailyTransform' which uses 'DailyEnvironment' and issues the commands 'dbt build' 'dbt run'. This job is run on a schedule so that it executes at 5:00 UTC on Tuesday through Saturday, after the markets have closed. This job runs the dbt model 'sp500_newtable.sql' which then recreates the 'gain' column in 'sp500_newtable'. 

- Once the new table has been created in BigQuery and updated each day with a batch, we will perform some analysis in Google Data Studio. First go to the <URL>, and then create a new data source. One then chooses the new table that was created ('sp500_newtable') and clicks on 'Connect'. 

- In Google Data Studio, we create a new field called 'Daily gains (%)' in order to plot a histogram of the distribution of daily gains. To do this, we go to 'Add a field', set the Field Name as 'gainbin' and then add the following formula: 

```
CASE 
    WHEN gain >-50 AND gain<=-3.25 THEN -3.5
    WHEN gain >-3.25 AND gain<=-2.75 THEN -3
    WHEN gain >-2.75 AND gain<=-2.25 THEN -2.5
    WHEN gain >-2.25 AND gain<=-1.75 THEN -2
    WHEN gain >-1.75 AND gain<=-1.25 THEN -1.5
    WHEN gain >-1.25 AND gain<=-0.75 THEN -1.0
    WHEN gain >-0.75 AND gain<=-0.25 THEN -0.5
    WHEN gain >-0.25 AND gain<=0.25 THEN 0.0
    WHEN gain >0.25 AND gain<=0.75 THEN 0.5
    WHEN gain >0.75 AND gain<=1.25 THEN 1.0
    WHEN gain >1.25 AND gain<=1.75 THEN 1.5
    WHEN gain >1.75 AND gain<=2.25 THEN 2.0
    WHEN gain >2.25 AND gain<=2.75 THEN 2.5
    WHEN gain >2.75 AND gain<=3.25 THEN 3.0
    WHEN gain >3.25 AND gain<=50 THEN 3.5
ELSE 0 END
```

- We now plot a time series of the S&P500 index price along with a histogram of the daily gains over the course of the time range of the data. 
                               
![My Dashboard](https://github.com/waggjeff/DE-Zoomcamp-FinalProject/blob/main/Analysis_of_Historical_S%26P500_Data-1.png "S&P500 dashboard made in Google Data Studio")
