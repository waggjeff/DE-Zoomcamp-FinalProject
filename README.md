## Data Science Bootcamp Final Project: An Analysis of Financial Data from the NYSE
### Jeff Wagg - March, 2023

#### Data

In order to test our pipeline, we use data from the Kaggle financial datasets for the New York Stock Exchange (NYSE). The file we will use is: 

prices.csv: Daily stock prices for NYSE equities including opening closing price and daily stock volume. The data cover the time interval from 2010 until the end of 2016. 

The file can be found here: https://www.kaggle.com/datasets/dgawlik/nyse

#### Data Pipeline

For the pipeline itself, we will use Docker containers to copy the datasets from the csv files and upload them into the Google Cloud Platform (GCP). The processing will be done in batches so that in the future we can process new data on a daily basis. Data will first be uploaded into a datalake (Google storage Buckets) before being transported to a data warehouse (Bigquery) where it will be transformed. 

#### Dashboard

The tranformed data will be plotted in a dashboard using Google Studio. We plot ... 

#### Processing Steps

- download data from site 

- create a project in GCP and download the json file

- create a prefect account and copy the requirements from here: https://github.com/discdiver/prefect-zoomcamp/blob/main/requirements.txt

- run 'pip install -r requirements.txt' and 'prefect orion start' in your conda environment on on the shell command line 

- run 'etl_web_to_gcs_jfw.py' to upload a parquet file version of the data to GCS (make sure that the json file is in the working dir)

- run 'etl_gcs_to_bq_jfw.py' to copy the data from 

- create a new dbt project with BigQuery as the data warehosue. In this case I called it stock_analysis_jfw. Upload the json key file to connect to BigQuery 

- Once DBT project is inintialized, click on 'Develop'

- create a branch of main repository and make a model in the models directory by creating a new file. In this case, the model updates the 'mythic-byway-375404.stocks.nyse' table in BigQuery by adding a new column 'gain', which is the percentage daily gain for each stock on a given day. A new table is then written to the Dataset called: 'nyse_newtable'. 

- Once the new table has been created in BigQuery, we will perform some analysis in Google Data Studio. First go to the <URL>, and then create a new data source. One then chooses the new table that was created ('nyse_newtable') and choose 'Connect'. 
