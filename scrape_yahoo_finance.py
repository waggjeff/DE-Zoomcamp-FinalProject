import yfinance as yf
import pandas as pd
import numpy as np
import datetime as dt
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def append_bq(df: pd.DataFrame, tablename: str) -> None:
    """Write DataFrame intp BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    
    df.to_gbq(
        destination_table="stocks."+tablename,
        project_id="mythic-byway-375404",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )
    

@flow()
def etl_yfinance_to_bq():
    """Main ETL flow to load data from Yahoo into Big Query"""

    # download the latest S&p500 data from yahoo finance (does not return
    # adjusted close )
    sp500 = yf.Ticker("^GSPC")
    last = sp500.history(period="1d")
    openv = last.values[0][0]
    high = last.values[0][1]
    low = last.values[0][2]
    close = last.values[0][3]
    vol = last.values[0][4]
    yesterday =  dt.date.today() - dt.timedelta(days=1)
    yes_str = yesterday.strftime('%Y-%m-%d')
    df = pd.DataFrame({'Date': [yes_str+' 00:00:00 UTC'], 'Open': [openv],
                           'High': [high], 'Low': low, 'Close': [close],
                           'Volume': [vol]
        })
    df['Date']= pd.to_datetime(df['Date'])
    
    append_bq(df,tablename='sp500')

if __name__ == "__main__":
    etl_yfinance_to_bq()





