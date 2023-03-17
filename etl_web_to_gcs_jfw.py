from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

# This routine is used to read the file from local disk
@task(retries=3)
def fetch(dataset: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    #if randint(0, 1) > 0:
    #    raise Exception
    
    df = pd.read_csv(dataset+'.csv')
    return df

# This routine is deprecated for final project
@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues """
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = pd.to_datetime(df['Date'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

def write_local(df: pd.DataFrame, datasetdir: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"{datasetdir}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return 
        
@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    datadir = './Data/'
    file_price = 'prices'
    file_fundamentals = 'fundamentals'
    file_sp500 = 'SP500_10yr'
    
    df = fetch(datadir+file_price)
    path = write_local(df, datadir, file_price)
    write_gcs(path)

    df2 = fetch(datadir+file_fundamentals)
    df2 = df2.drop(['Unnamed: 0'], axis=1)
    path2 = write_local(df2, datadir, file_fundamentals)
    write_gcs(path2)

    df3 = fetch(datadir+file_sp500)
    df4 = clean(df3)
    path3 = write_local(df4, datadir, file_sp500)
    write_gcs(path3)
    
if __name__ == '__main__':
    etl_web_to_gcs()
