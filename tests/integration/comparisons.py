# %%
# INITIALIZATION
import requests
import json
import pandas as pd
import subprocess
import os
from tabulate import tabulate
from dotenv import load_dotenv
import re
load_dotenv('.env')

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
def upload_dataframe_to_bigquery(dataframe, dataset, table_name):

    # Load the DataFrame into BigQuery
    job = bq_client.load_table_from_dataframe(
                      dataframe=dataframe,
                      destination=f"{gcloud_project_id}.{dataset}.{table_name}",
                      job_config=bigquery.LoadJobConfig(
                                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                          )
                    )

    # Wait for the load job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {dataset}.{table_name}")


def generate_metricflow_results(mf_command, results_table):

    # Defined temporary output for results
    mf_command = mf_command + f" --csv logs/{results_table}.csv"

    # Run the dbt command
    result = subprocess.run(mf_command.split(), capture_output=True, text=True, cwd='dbt/')

    # Check if the command was successful
    if result.returncode != 0:
        print(f"Error occurred while executing command: {result.stderr}")

    # Load the CSV file into a DataFrame
    df = pd.read_csv(f'dbt/logs/{results_table}.csv', header=None)

    # Add column_1, column_2, etc column headers
    num_columns = df.shape[1]
    df.columns = [f'column_{i+1}' for i in range(num_columns)]

    upload_dataframe_to_bigquery(dataframe=df,
                                 dataset='mf_query_results',
                                 table_name=results_table)


def generate_looker_results(explore, fields, results_table):

    lkr_query = lkr_models.WriteQuery(model='dbt_slt', view=explore, fields=fields, limit=-1)
    response = lkr_sdk.run_inline_query("csv", lkr_query)
    df = pd.read_csv(StringIO(response))

    # Clean column names
    df.columns = [re.sub(r'[^a-zA-Z0-9_%]', '_', col) for col in df.columns]

    upload_dataframe_to_bigquery(dataframe=df,
                                 dataset='lkr_query_results',
                                 table_name=results_table)


def generate_cube_results(query, results_table):

    response = requests.get(url=os.getenv('CUBE_URL'),
                            headers={
                              "Authorization": os.getenv('CUBE_AUTH_TOKEN')
                            },
                            params={
                              "query": json.dumps(query)
                            })

    if response.status_code == 200:
      response_json = response.json()
      data = response_json.get("data", [])
      df = pd.DataFrame(data)

      # Clean column names
      df.columns = [re.sub(r'[^a-zA-Z0-9_%]', '_', col) for col in df.columns]

      upload_dataframe_to_bigquery(dataframe=df,
                                   dataset='cube_query_results',
                                   table_name=results_table)

    else:
      response.raise_for_status()



def do_query_results_match(results_query1, results_query2):

    query = f"""
    WITH results_schema1_results AS (
      {results_query1}
    ),
    results_schema2_results AS (
      {results_query2}
    ),
    results_schema1_except_results_schema2 AS (
      SELECT * FROM results_schema1_results
      EXCEPT DISTINCT
      SELECT * FROM results_schema2_results
    ),
    results_schema2_except_results_schema1 AS (
      SELECT * FROM results_schema2_results
      EXCEPT DISTINCT
      SELECT * FROM results_schema1_results
    )
    SELECT '`{results_query1}`' AS source, * FROM results_schema1_except_results_schema2
      UNION ALL
    SELECT '`{results_query2}`' AS source, * FROM results_schema2_except_results_schema1
    """

    query_job = bq_client.query(query)
    results = query_job.result()
    mismatches = list(results)

    if len(mismatches) == 0:
        print(f"Pass - `{results_query1}` matches `{results_query2}`")
        return True
    else:
        print(f"Fail - `{results_query1}` does not match `{results_query2}`")

        # Convert mismatches to a list of dictionaries
        mismatch_dicts = [dict(row) for row in mismatches]

        # Print the mismatches using tabulate
        print(tabulate(mismatch_dicts, headers="keys", tablefmt="pretty"))

        return False


# %%
# SIMPLE METRIC COMPARISONS
generate_metricflow_results(mf_command='mf query --metrics order_total', results_table='simple_metric')

generate_looker_results(explore='orders', fields=['orders.order_total'], results_table='simple_metric')
do_query_results_match(results_query1='select * from mf_query_results.simple_metric',
                       results_query2='select * from lkr_query_results.simple_metric')

generate_cube_results(query={"measures": ["orders.order_total"]}, results_table='simple_metric')
do_query_results_match(results_query1='select * from mf_query_results.simple_metric',
                       results_query2='select * from cube_query_results.simple_metric')

# %%
# SIMPLE METRIC WITH CATEGORY FILTER
generate_metricflow_results(mf_command='mf query --metrics food_orders', results_table='simple_metric_with_category_filter')

generate_looker_results(explore='orders', fields=['orders.food_orders'], results_table='simple_metric_with_category_filter')
do_query_results_match(results_query1='select * from mf_query_results.simple_metric_with_category_filter',
                       results_query2='select * from lkr_query_results.simple_metric_with_category_filter')

generate_cube_results(query={"measures": ["orders.food_orders"]}, results_table='simple_metric_with_category_filter')
do_query_results_match(results_query1='select * from mf_query_results.simple_metric_with_category_filter',
                       results_query2='select * from cube_query_results.simple_metric_with_category_filter')

# %%
# ROLLING TIME WINDOW METRIC
generate_metricflow_results(mf_command="mf query --metrics orders_last_7_days --start-time '2016-09-01' --end-time '2016-09-30' --group-by metric_time --order metric_time", results_table='rolling_window_metric')

# Native rolling time window metric is not supported in Looker :-( 

generate_cube_results(query={"measures": ["orders.orders_last_7_days"], "timeDimensions": [{"dimension": "orders.ordered_at","granularity": "day","dateRange": [  "2016-09-01",  "2016-09-30"] } ],  "order": { "orders.ordered_at": "asc" }}, results_table='rolling_window_metric')
do_query_results_match(results_query1='select cast(column_1 as datetime), column_2 from mf_query_results.rolling_window_metric',
                       results_query2='select cast(orders_ordered_at_day as datetime), orders_orders_last_7_days from cube_query_results.rolling_window_metric')

# %%
# FILTERED RATIO METRIC
generate_metricflow_results(mf_command="mf query --metrics pc_drink_orders_for_returning_customers --group-by location__location_name", results_table='filtered_ratio_metric')

generate_looker_results(explore='orders', fields=['locations.location_name', 'orders.pc_drink_orders_for_returning_customers'], results_table='filtered_ratio_metric')
do_query_results_match(results_query1='select * from mf_query_results.filtered_ratio_metric',
                       results_query2='select * from lkr_query_results.filtered_ratio_metric')

generate_cube_results(query={"measures": ["orders.pc_drink_orders_for_returning_customers"], "dimensions": ["locations.location_name"]}, results_table='filtered_ratio_metric')
do_query_results_match(results_query1='select column_1, round(column_2, 15) from mf_query_results.filtered_ratio_metric',
                       results_query2='select locations_location_name, round(orders_pc_drink_orders_for_returning_customers, 15) from cube_query_results.filtered_ratio_metric')
# %%
# FILTERED RATIO METRIC #2
generate_metricflow_results(mf_command="mf query --metrics pc_deliveries_with_5_stars --group-by delivery_person__full_name", results_table='filtered_ratio_metric_2')

generate_looker_results(explore='deliveries', fields=['delivery_people.full_name', 'deliveries.pc_deliveries_with_5_stars'], results_table='filtered_ratio_metric_2')
do_query_results_match(results_query1='select * from mf_query_results.filtered_ratio_metric_2',
                       results_query2='select * from lkr_query_results.filtered_ratio_metric_2')

generate_cube_results(query={"measures": ["deliveries.pc_deliveries_with_5_stars"], "dimensions": ["delivery_people.full_name"]}, results_table='filtered_ratio_metric_2')
do_query_results_match(results_query1='select column_1, round(column_2, 14) from mf_query_results.filtered_ratio_metric_2',
                       results_query2='select delivery_people_full_name, round(deliveries_pc_deliveries_with_5_stars, 14) from cube_query_results.filtered_ratio_metric_2')
# %%
