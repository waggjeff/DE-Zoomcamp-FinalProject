## Data Engineering Bootcamp Final Project: An Analysis of S&P500 Index Data
### Jeff Wagg - March, 2023

The Standard and Poor's 500 (S&P500) stock index tracks the performance of 500 large US companies. It was founded in the late 1950s and can be a good indicator of the economic health of the nation. Each day, the price of the index fund is updated and it would be useful to have an automatic means of calculating the typical gains achieved on a daily basis. To achieve this, we create a pipeline that uses historical price data (Open and Close price) from the S&P500, and processes these data in a pipeline. The pipeline is capable of calculating the daily gain of the index, and updates the tables in the Data Warehouse (BigQuery) each day (batch mode). 

#### Data

We use data from Yahoo Finance which contains the historical data over a user-specified timeframe (search for symbol '^GSPC'). The file we will use is: 

SP500_10yr.csv: Daily opening and closing prices along with the trading volume for the S&P500. The data cover the time interval from March, 2013 until March, 2023. 

The file can be created by scraping the data found here: https://finance.yahoo.com/quote/%5EGSPC/history/

#### Data Pipeline

For the pipeline itself, we will use Docker containers to copy the datasets from the csv files and upload them into the Google Cloud Platform (GCP). The processing will be done in batches so that in the future we can process new data on a daily basis. Data will first be uploaded into a datalake (Google storage Buckets) before being transported to a data warehouse (Bigquery) where it is then transformed using a DBT model. 

#### Dashboard

The tranformed data are plotted in a dashboard using Google Date Studio.  

#### Processing Steps

- download data from site 

- create a project in GCP and download the json file

- create a prefect account and copy the requirements from here: https://github.com/discdiver/prefect-zoomcamp/blob/main/requirements.txt

- run 'pip install -r requirements.txt' and 'prefect orion start' in your conda environment on on the shell command line 

- run 'etl_web_to_gcs_jfw.py' to upload a parquet file version of the data to GCS (make sure that the json file is in the working dir)

- run 'etl_gcs_to_bq_jfw.py' to copy the data from 

- create a new dbt project with BigQuery as the data warehosue. In this case I called it stock_analysis_jfw. Upload the json key file to connect to BigQuery 

- Once DBT project is inintialized, click on 'Develop'

- create a branch of main repository and make a model in the models directory by creating a new file called 'sp500_newtable.sql'. In this case, the model updates the 'mythic-byway-375404.stocks.sp500' table in BigQuery by adding a new column 'gain', which is the percentage daily gain for each stock on a given day. A new table is then written to the Dataset called: 'sp500_newtable'. 

- Now, once the BigQuery table is created, we want to be able to update this with the latest S&P500 data each day. That is where the batch job comes in. A python script called 'scrape_yahoo_finance.py' uses the 'yfinance' package to download the latest markt data from Yahoo Finance. It then runs a prefect flow to append the latest data to the 'sp500' table in BigQuery. The dbt model 'sp500_newtable.sql' can then be rerun to create the 'gain' column. 

- Once the new table has been created in BigQuery and updated each day with a batch, we will perform some analysis in Google Data Studio. First go to the <URL>, and then create a new data source. One then chooses the new table that was created ('sp500_newtable') and choose 'Connect'. 

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
