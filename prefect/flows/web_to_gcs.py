import os, requests, shutil, zipfile
from typing import Tuple
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from google.cloud import storage
# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload link.
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB

@task(task_run_name='get_url_and_filename_{year}-{month:02}', log_prints=True)
def get_url_and_filename(year: int, month: int) -> Tuple[str, str]:
    """
    Returns the URL and filename for a given year and month.

    Args:
        year (int): The year to retrieve data for.
        month (int): The month to retrieve data for.

    Returns:
        Tuple[str, str]: A tuple containing the URL and filename.
    """
    
    if year == 2022 and month == 11:
        # There is a typo in the filename for Nov 2022
        filename = f'{year}{month:02}-baywheeels-tripdata'
    else:    
        filename = f'{year}{month:02}-baywheels-tripdata'

    url = f'https://s3.amazonaws.com/baywheels-data/{filename}.csv.zip'
    print(url)
    
    return url, filename


@task(task_run_name='download_csv_{kwargs[year]}-{kwargs[month]:02}', retries=3, log_prints=True)
def download_csv(url: str, filename: str, **kwargs) -> Path:
    """
    Downloads a compressed CSV file from a given URL and saves it in the 'data_zipped' directory.

    Args:
        url (str): The URL to download the CSV file from.
        filename (str): Name of the CSV file.
        **kwargs: The 'year' and 'month' values are used to generate the task run name.

    Returns:
        Path: The full path to the downloaded file.
    """
    
    dest_dir: str='data_zipped'
    if not os.path.exists(f'./{dest_dir}'):
        os.makedirs(os.path.join('.', dest_dir))
    
    zip_file_path = Path(os.path.join('.', dest_dir, f'{filename}.csv.zip'))

    with requests.get(url, stream=True) as r:
        with open(zip_file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    print(f'csv.zip file successfully downloaded to: {zip_file_path}')
    return zip_file_path


@task(task_run_name='load_df_{kwargs[year]}-{kwargs[month]:02}', log_prints=True)
def load_df(filename: str, zip_file_path: Path, **kwargs) -> pd.DataFrame:
    """
    Load a pandas DataFrame from a file in a zip archive.

    Args:
        filename (str): The name of the file to read the DataFrame from.
        filepath (Path): The full path to the file containing the DataFrame.
        **kwargs: The 'year' and 'month' values are used to generate the task run name.

    Returns:
        pd.DataFrame: The DataFrame loaded from the specified file.
    """
    
    with zipfile.ZipFile(zip_file_path) as z:
        with z.open(f'{filename}.csv') as f:
            df = pd.read_csv(f, parse_dates=['started_at', 'ended_at'])
    
    print(f'{filename}.csv shape: {df.shape}')
    return df


@task(task_run_name='upload_df_{year}-{month:02}', timeout_seconds=300)
def upload_df(df: pd.DataFrame, year: int, month: int) -> None:
    """Uploads a pandas DataFrame to Google Cloud Storage (GCS) as a parquet file.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.
        year (int): The year of the data.
        month (int): The month of the data.
    """
    
    # Creating filename again due to inconsistent names in the raw data
    filename = f'{year}{month:02}-baywheels-tripdata'
    gcs_file_path = Path(f'{year}') / f'{filename}.parquet'
    
    block_name ='sf-bay-area'
    gcs_bucket = GcsBucket.load(block_name)
    
    gcs_bucket.upload_from_dataframe(
        df=df,
        to_path=gcs_file_path,
        serialization_format='parquet_snappy'
    )

 
@flow(flow_run_name='web_to_gcs_subflow_{year}-{month:02}')
def web_to_gcs_subflow(year: int, month: int):
    """
    Download CSV from the web, and upload the DataFrame to GCS as a Parquet file.

    Args:
        year (int): The year for which to download the data.
        month (int): The month (1-12) for which to download the data.
    """
    
    url, filename = get_url_and_filename(year, month)
    zip_file_path = download_csv(url, filename, year=year, month=month)
    
    df = load_df(filename, zip_file_path, year=year, month=month)
    upload_df(df, year, month)
    

@flow()
def web_to_gcs_flow(years: list[int] = [2021, 2022], months: list[int] = list(range(1, 13))):
    """
    A Prefect flow that orchestrates the web_to_gcs_subflow.

    Args:
        years (list[int]): Years to iterate over
        months (list[int]): Months to iterate over
    """
    
    for year in years:
        for month in months:
            web_to_gcs_subflow(year, month)


if __name__ == '__main__':
    web_to_gcs_flow()