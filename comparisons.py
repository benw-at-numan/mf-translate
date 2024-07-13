# %%
# INITIALIZATION
import pandas as pd
import subprocess
import os
from dotenv import load_dotenv
load_dotenv('.env')
from tabulate import tabulate

# Set Google Cloud Credentials
from google.cloud import bigquery
from google.oauth2 import service_account
gcloud_project_id = 'fresh-iridium-428713-j5'
credentials = service_account.Credentials.from_service_account_file(
        os.getenv('GCLOUD_CREDENTIALS_PATH') 
    )
bq_client = bigquery.Client(credentials=credentials, project=gcloud_project_id)

# Initialise Looker SDK
import looker_sdk
from looker_sdk import models40 as lkr_models
from io import StringIO
lkr_sdk = looker_sdk.init40("./looker/looker.ini")



# %%
# DEFINE FUNCTIONS
def upload_dataframe_to_bigquery(dataframe, table_full_id):

     # Define the job configuration for the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load the DataFrame into BigQuery
    job = bq_client.load_table_from_dataframe(dataframe, table_full_id, job_config=job_config)

    # Wait for the load job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_full_id}")




def generate_metricflow_results(mf_command, table_name_for_results):

    # Defined temporary output for results
    mf_command = mf_command + f" --csv logs/{table_name_for_results}.csv"

    # Run the dbt command
    result = subprocess.run(mf_command.split(), capture_output=True, text=True, cwd='dbt/')

    # Check if the command was successful
    if result.returncode != 0:
        print(f"Error occurred while executing command: {result.stderr}")

    # Load the CSV file into a DataFrame
    df = pd.read_csv(f'dbt/logs/{table_name_for_results}.csv', header=None)

    # Add column_1, column_2, etc column headers
    num_columns = df.shape[1]
    df.columns = [f'column_{i+1}' for i in range(num_columns)]

    upload_dataframe_to_bigquery(dataframe=df,
                                 table_full_id=f"{gcloud_project_id}.mf_query_results.{table_name_for_results}")



def generate_looker_results(explore, fields, table_name_for_results):

    lkr_query = lkr_models.WriteQuery(model='dbt_slt', view=explore, fields=fields, limit=-1)
    response = lkr_sdk.run_inline_query("csv", lkr_query)

    df = pd.read_csv(StringIO(response))

    upload_dataframe_to_bigquery(dataframe=df,
                                table_full_id=f"{gcloud_project_id}.lkr_query_results.{table_name_for_results}")



def do_query_results_match(table_name, result_schema1, result_schema2):

    query = f"""
    WITH result_schema1_results AS (
      SELECT * FROM `{result_schema1}.{table_name}`
    ),
    result_schema2_results AS (
      SELECT * FROM `{result_schema2}.{table_name}`
    ),
    result_schema1_except_result_schema2 AS (
      SELECT * FROM result_schema1_results
      EXCEPT DISTINCT
      SELECT * FROM result_schema2_results
    ),
    result_schema2_except_result_schema1 AS (
      SELECT * FROM result_schema2_results
      EXCEPT DISTINCT
      SELECT * FROM result_schema1_results
    )
    SELECT '`{result_schema1}`' AS source, * FROM result_schema1_except_result_schema2
      UNION ALL
    SELECT '`{result_schema2}`' AS source, * FROM result_schema2_except_result_schema1
    """

    query_job = bq_client.query(query)
    results = query_job.result()
    mismatches = list(results)

    if len(mismatches) == 0:
        print(f"Pass - {result_schema1}.{table_name} matches {result_schema1}.{table_name}")
        return True
    else:
        print(f"Fail - {result_schema1}.{table_name} does not match {result_schema1}.{table_name}")

        # Convert mismatches to a list of dictionaries
        mismatch_dicts = [dict(row) for row in mismatches]

        # Print the mismatches using tabulate
        print(tabulate(mismatch_dicts, headers="keys", tablefmt="pretty"))

        return False


# %%
# RUN COMPARISONS
generate_metricflow_results(mf_command='mf query --metrics order_total', table_name_for_results='simple_metric')
generate_looker_results(explore='orders', fields=['orders.order_total'], table_name_for_results='simple_metric')
do_query_results_match(table_name='simple_metric', result_schema1='mf_query_results', result_schema2='lkr_query_results')
# %%
