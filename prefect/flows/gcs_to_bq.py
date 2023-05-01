from pathlib import Path
from typing import Union
import pandas as pd
import pandera as pa
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(task_run_name='download_blob_{year}-{month:02}', retries=3)
def download_blob(year: int, month: int) -> Path:
    """
    Downloads the Parquet file from GCS to 'data' directory.

    Args:
        year (int): The year of the data to download.
        month (int): The month (1-12) of the data to download.
    
    Returns:
        Path: The path to the downloaded file.
    """
    
    dest_dir = 'data'
    gcs_path = f'{year}/{year}{month:02}-baywheels-tripdata.parquet.snappy'
    
    block_name = 'sf-bay-area'
    gcs_bucket = GcsBucket.load(block_name)
    
    gcs_bucket.get_directory(
        from_path=gcs_path,
        local_path=f'./{dest_dir}/'
        )
    
    return Path(f'./{dest_dir}/{gcs_path}')


@task(task_run_name='load_and_validate_df_{kwargs[year]}-{kwargs[month]:02}', log_prints=True)
def load_and_validate_df(filepath: Path, **kwargs) -> Union[pd.DataFrame, bool]:
    """
    Load DataFrame from a file path and validate it against a Pandera schema.

    Args:
        filepath (Path): Filepath to load the DataFrame from.
        **kwargs: The 'year' and 'month' values are used to generate the task run name.

    Returns:
        Union[pd.DataFrame, bool]: The DataFrame if validation succeeds, else 'False'.
    """
    
    pa_schema = pa.DataFrameSchema({
 
        # str, unique, not null
        'ride_id': pa.Column(pa.String, nullable=False, checks=[
            pa.Check(lambda ride_id: len(df.ride_id.unique()) == df.ride_id.shape[0])
        ]),
        
        # categorical, not null
        'rideable_type': pa.Column(pa.String, nullable=False, checks=[
            pa.Check.isin(['electric_bike', 'classic_bike', 'docked_bike'])
        ]),
        
        # datetime, not null
        'started_at': pa.Column(pa.DateTime, nullable=False),
        'ended_at': pa.Column(pa.DateTime, nullable=False),
        
        # str, nullable
        'start_station_name': pa.Column(pa.String, nullable=True),
        'start_station_id': pa.Column(pa.String, nullable=True),
        'end_station_name': pa.Column(pa.String, nullable=True),
        'end_station_id': pa.Column(pa.String, nullable=True),
        
        # float, not null
        'start_lat': pa.Column(pa.Float, nullable=False),
        'start_lng': pa.Column(pa.Float, nullable=False),
        
        # float, nullable
        'end_lat': pa.Column(pa.Float, nullable=True),
        'end_lng': pa.Column(pa.Float, nullable=True),

        # categorical, not null
        'member_casual': pa.Column(pa.String, nullable=False, checks=[
            pa.Check.isin(['casual', 'member'])])
    })
    
    try:
        df = pd.read_parquet(filepath)
        validated_df = pa_schema.validate(df)
    except pa.errors.SchemaErrors as err:
        print(f'Validation failed against the Pandera schema for {kwargs["year"]}{kwargs["month"]:02}-baywheels-tripdata:\n{err}')
        return False
    except Exception as e:
        print(f'Error validating dataframe: {e}')
        return False
    else:
        print(f'Validation successful against the Pandera schema for {kwargs["year"]}{kwargs["month"]:02}-baywheels-tripdata.parquet')
        print(f'DataFrame shape: {validated_df.shape}')
        return validated_df


@task(task_run_name='upload_to_bq_{kwargs[year]}-{kwargs[month]:02}', log_prints=True)
def upload_to_bq(df: pd.DataFrame, block_name: str='sf-bay-area', **kwargs) -> None:
    """
    Upload a Pandas DataFrame to BigQuery.

    Args:
        df (pandas.DataFrame): The DataFrame to upload.
        **kwargs: Additional keyword arguments. The 'year' and 'month' values are used to generate the task run name.
    
    Returns:
        None
    """
    
    bq_schema = [
        {'name': 'ride_id', 'type': 'STRING', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'rideable_type', 'type': 'STRING', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'started_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'ended_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'start_station_name', 'type': 'STRING', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'start_station_id', 'type': 'STRING', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'end_station_name', 'type': 'STRING', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'end_station_id', 'type': 'STRING', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'start_lat', 'type': 'FLOAT', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'start_lng', 'type': 'FLOAT', 'mode': 'REQUIRED', 'is_nullable': False},
        {'name': 'end_lat', 'type': 'FLOAT', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'end_lng', 'type': 'FLOAT', 'mode': 'NULLABLE', 'is_nullable': True},
        {'name': 'member_casual', 'type': 'STRING', 'mode': 'REQUIRED', 'is_nullable': False}
        ]
    
    block_name = 'sf-bay-area'
    gcp_creds = GcpCredentials.load(block_name)
    
    dataset_name = 'sf_bay_area_tripdata'
    table_name = 'trips_data_all'
    
    try:
        df.to_gbq(
            destination_table=f'{dataset_name}.{table_name}',
            project_id=gcp_creds.project,
            credentials=gcp_creds.get_credentials_from_service_account(),
            if_exists='append',
            table_schema=bq_schema
        )
    except Exception as e:
        print(f'An error occurred while writing to BigQuery: {e}')
        raise
    else:
        print('Data successfully written to BigQuery')


@flow(flow_run_name='gcs_to_bq_subflow_{year}-{month:02}', log_prints=True)
def gcs_to_bq_subflow(year: int, month: int):
    """
    Download parquet from GCS and upload the DataFrame to BigQuery.

    Args:
        year (int): The year of the data.
        month (int): The month of the data.
    """
    
    filepath = download_blob(year, month)
    validated_df = load_and_validate_df(filepath, year=year, month=month)
    
    if isinstance(validated_df, pd.DataFrame):
        upload_to_bq(validated_df, year=year, month=month)
    else:
        raise Exception('Validation failed or returned unexpected result. Unable to upload to BigQuery.')
        

@flow
def gcs_to_bq_flow(years: list[int] = [2021, 2022], months: list[int] = list(range(1, 13))):
    """
    A Prefect flow that orchestrates the gcs_to_bq_subflow.

    Args:
    years (list[int]): A list of years to iterate over. Defaults to [2021, 2022].
    months (list[int]): A list of months to iterate over. Defaults to all 12 months
    """
    
    for year in years:
        for month in months:
            gcs_to_bq_subflow(year, month)


if __name__ == '__main__':
    gcs_to_bq_flow()