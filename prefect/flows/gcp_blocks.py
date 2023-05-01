import os
from pathlib import Path
from prefect import flow
from prefect_gcp import GcpCredentials, GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse

@flow(log_prints=True)
def gcp_blocks():
    block_name = 'sf-bay-area'
    google_dir = Path.cwd().parent / '.google'
    json_files = list(google_dir.glob("*.json"))
    
    if json_files:
        service_account_filepath = json_files[0]
        print(f"Using service account file: {service_account_filepath}")
    else:
        service_account_filepath = input('Enter service account file path: ')
        service_account_filepath = Path(os.path.abspath(service_account_filepath))
    
    try:
        gcp_creds = GcpCredentials(service_account_file=service_account_filepath)
        gcp_creds.save(name=f'{block_name}', overwrite=True)
    
    except Exception as e:
        raise e
    
    else:
        print(f'GCP Credentials block: "{block_name}" created')
        gcs_bucket = GcsBucket(bucket = f'data_lake_{gcp_creds.project}',
                                gcp_credentials = GcpCredentials.load(f'{block_name}'))
        
        gcs_bucket.save(name=f'{block_name}', overwrite=True)
        print(f'GCS Bucket block: "{block_name}" created')
        
        bq_block = BigQueryWarehouse(gcp_credentials=gcp_creds)
        bq_block.save(name=block_name, overwrite=True)
        print(f'BigQuery Warehouse block: "{block_name}" created')
    

if __name__ == '__main__':
    gcp_blocks()