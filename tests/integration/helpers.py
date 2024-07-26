# tests/helpers.py
import subprocess
import pandas as pd
import requests
import json
import re
from tabulate import tabulate
from google.cloud import bigquery
from looker_sdk import models40 as lkr_models
from io import StringIO
import os

def upload_dataframe_to_bigquery(dataframe, dataset, table_name, bq_client, gcloud_project_id):
    job = bq_client.load_table_from_dataframe(
        dataframe=dataframe,
        destination=f"{gcloud_project_id}.{dataset}.{table_name}",
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    )
    job.result()
    print(f"Loaded {job.output_rows} rows into {dataset}.{table_name}")

def generate_metricflow_results(mf_command, results_table, bq_client, gcloud_project_id):
    mf_command = mf_command + f" --csv logs/{results_table}.csv"
    result = subprocess.run(mf_command.split(), capture_output=True, text=True, cwd='test_environments/dbt/')
    if result.returncode != 0:
        print(f"Error occurred while executing command: {result.stderr}")
    df = pd.read_csv(f'test_environments/dbt/logs/{results_table}.csv', header=None)
    num_columns = df.shape[1]
    df.columns = [f'column_{i+1}' for i in range(num_columns)]
    upload_dataframe_to_bigquery(df, 'mf_query_results', results_table, bq_client, gcloud_project_id)

def generate_looker_results(explore, fields, results_table, lkr_sdk, bq_client, gcloud_project_id):
    lkr_query = lkr_models.WriteQuery(model='dbt_slt', view=explore, fields=fields, limit=-1)
    response = lkr_sdk.run_inline_query("csv", lkr_query)
    df = pd.read_csv(StringIO(response))
    df.columns = [re.sub(r'[^a-zA-Z0-9_%]', '_', col) for col in df.columns]
    upload_dataframe_to_bigquery(df, 'lkr_query_results', results_table, bq_client, gcloud_project_id)

def generate_cube_results(query, results_table, bq_client, gcloud_project_id):
    response = requests.get(
        url=os.getenv('CUBE_URL'),
        headers={"Authorization": os.getenv('CUBE_AUTH_TOKEN')},
        params={"query": json.dumps(query)}
    )
    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get("data", [])
        df = pd.DataFrame(data)
        df.columns = [re.sub(r'[^a-zA-Z0-9_%]', '_', col) for col in df.columns]
        upload_dataframe_to_bigquery(df, 'cube_query_results', results_table, bq_client, gcloud_project_id)
    else:
        response.raise_for_status()

def do_query_results_match(results_query1, results_query2, bq_client):
    query = f"""
    WITH results_schema1_results AS ({results_query1}),
    results_schema2_results AS ({results_query2}),
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
        mismatch_dicts = [dict(row) for row in mismatches]
        print(tabulate(mismatch_dicts, headers="keys", tablefmt="pretty"))
        return False