import os
from prefect import flow
from prefect_gcp.bigquery import BigQueryWarehouse

@flow(log_prints=True)
def bq_table():
    """
    Create or replace a BigQuery table with a specified name and schema.
    """
    
    block_name = 'sf-bay-area'
    dataset_name = 'sf_bay_area_tripdata'
    table_name = 'trips_data_all'
    partitioned_table_name = 'trips_data_partitioned'
    
    with BigQueryWarehouse.load(block_name) as warehouse:
        project_id = warehouse.gcp_credentials.project
        
        operation = f'''
            CREATE OR REPLACE TABLE {project_id}.{dataset_name}.{partitioned_table_name}
            PARTITION BY
                DATE(started_at)
            AS
            SELECT *
            FROM {project_id}.{dataset_name}.{table_name};
        '''

        warehouse.execute(operation)
        print(f'{project_id}.{dataset_name}.{partitioned_table_name} created')

if __name__ == '__main__':
    bq_table()