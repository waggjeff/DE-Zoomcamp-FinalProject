from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(datadir: str, filename: str) -> Path:
    """ Download data from GCS"""
    gcs_path = f"{datadir}/{filename}"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return Path(f"{gcs_path}.parquet")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning Example"""
    df = pd.read_parquet(path)
    if 'Ticker Symbol' in df.columns:
        df.rename(columns={"Ticker Symbol": "symbol"}, inplace=True)

    if 'symbol' in df.columns:
        print(f"pre missing data: {df['symbol'].isna().sum()}")
        df['symbol'].fillna(0, inplace=True)
        print(f"post missing data count: {df['symbol'].isna().sum()}")
        
    return df
    
@task()
def write_bq(df: pd.DataFrame, tablename: str) -> None:
    """Write DataFrame intp BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    
    df.to_gbq(
        destination_table="stocks."+tablename,
        project_id="mythic-byway-375404",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='replace'
    )
    

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    datadir = './Data'
    file_price = 'prices'
    file_fundamentals = 'fundamentals'
    file_sp500 = 'SP500_10yr'
    
    path = extract_from_gcs(datadir, file_sp500)
    df = transform(path)
    write_bq(df,tablename='sp500')

if __name__ == "__main__":
    etl_gcs_to_bq()
