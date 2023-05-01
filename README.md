## SF Bay Area Tripdata: DE Zoomcamp capstone project

### Problem description
- Find out most popular stations, average trip duration and number of trips for those stations.
- Find out how trip count varies with start hour for every day in a week.
- Trip distribution drilled down by member type.

The dataset can be found [here](https://s3.amazonaws.com/baywheels-data/index.html)

### Steps to reproduce the project
#### 1. Create a Service Account File associated with a GCP project with the following roles assigned
- BigQuery Admin
- Storage Admin
- Storage Object Admin
- Viewer

- Copy the Service account file in `.google/`

#### 2. Terraform

- Install [terraform](https://developer.hashicorp.com/terraform/downloads)
- Switch to terraform directory using `cd terraform/`
- Run `terraform init` to initialize the directory and download required provider plugins.
- Run `terraform validate` to validate the configuration.
- Inside `variables.tf` provide default value for "project", which is the GCP project ID.
- Inside `main.tf` provide the service account file path.
`credentials = file("../.google/<service-account-key.json>")`
- Run `terraform plan` to create an execution plan for creating the resources.
- Run `terraform apply` to apply the changes proposed by `terraform plan`
- `terraform destroy` to tear down the resources after we're done.

#### 3. Prefect
- Switch to prefect directory using `cd prefect/`
- Run `python3 -m venv .venv` to create a Python virutal environment.
- Run `source .venv/bin/activate` to activate the virtual environment.
- Run `pip install -r requirements.txt` to install the required packages.
- Start the prefect server using `prefect server start`
- Open up another terminal, switch to the 'prefect/' directory and activate the virutal env
- Run `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api` to use a local prefect server instance
- Run `python flows/gcp_blocks.py` to create GCP blocks

##### Creating deployments
`prefect deployment build -n sf-bay-area flows/web_to_gcs.py:web_to_gcs_flow -a`
`prefect deployment build -n sf-bay-area flows/gcs_to_bq.py:gcs_to_bq_flow -a`
`prefect deployment build -n sf-bay-area flows/bq_table.py:bq_table -a`

- Run `prefect agent start -q 'default'` to start a prefect agent, which is needed to run the deployments.

- To run the deployments, open up yet another terminal, switch to 'prefect/' directory, activate the virtual env.
`prefect deployment run web-to-gcs-flow/sf-bay-area`
`prefect deployment run gcs-to-bq-flow/sf-bay-area`
`prefect deployment run bq-table/sf-bay-area`
- We can override the parameters to the deployments using the --params. E.g., `--params='{"years": [2021], "months":[3]}'`

#### 4. dbt

- Switch to dbt directory using `cd dbt/`
- Run `python3 -m venv .venv` to create a Python virutal environment.
- Run `source .venv/bin/activate` to activate the virtual environment.
- Run `pip install -r requirements.txt` to install the required packages.
- Run `dbt init` to setup the connection info and configure the dataset, this is the dataset used by dbt.
- Run `dbt debug` to validate the connection to BigQuery.
- Run `dbt deps` to install the required dbt packages.
- Modify `models/core/schema.yml` and `models/core/schema.yml` to include the database and the schema. Database is GCP project ID, and schema is the table name.
  ```yml
    # models/core/schema.yml

    sources:
    	- name: staging
		  database: main-audio-384108
		  schema: sf_bay_area_tripdata
  ```
  ```yml
    # models/core/schema.yml

    sources:
      - name: core
	    database: main-audio-384108
	    schema: dbt_shubhankar4
   ```

- Run `dbt run` to run all the models, we can run a specific model using the `--select <path/to/model.sql>`
- Run `dbt test` to test the models against the assumptions made about the data.
- Run `dbt docs generate` to generate the docs.
- Run `dbt docs serve` to view the docs

- The visualization can be found [here](./SF_Bay_Area_Report.pdf)