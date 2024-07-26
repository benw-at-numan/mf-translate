# tests/helpers.py
import subprocess
import pandas as pd
import requests
import json
from tabulate import tabulate
from looker_sdk import models40 as lkr_models
from io import StringIO
import os


def query_metricflow(metrics, group_by=None, order_by=None, start_time=None, end_time=None):

    # Convert metric array to comma separated string
    metrics_list = ','.join(metrics)

    # Define the dbt command
    mf_command = f"mf query --metrics {metrics_list} --csv logs/query_results.csv"

    if group_by:
      group_by_list = ','.join(group_by)
      mf_command += f" --group-by {group_by_list}"

    if order_by:
      order_by_list = ','.join(order_by)
      mf_command += f" --order {order_by_list}"

    if start_time:
      mf_command += f" --start-time '{start_time}'"

    if end_time:
      mf_command += f" --end-time '{end_time}'"

    # Run the dbt command
    result = subprocess.run(mf_command.split(), capture_output=True, text=True, cwd='dbt/')

    # Check if the command was successful
    if result.returncode != 0:
        print(f"Error occurred while executing command: {result.stderr}")

    # Load the CSV file into a DataFrame
    query_results_df = pd.read_csv(f'dbt/logs/query_results.csv', header=None)

    # Add column_1, column_2, etc column headers
    num_columns = query_results_df.shape[1]
    query_results_df.columns = [f'column_{i+1}' for i in range(num_columns)]

    return query_results_df


def query_looker(lkr_sdk, explore, fields, sorts=None):

    lkr_query = lkr_models.WriteQuery(model='dbt_slt', view=explore, fields=fields, sorts=sorts, limit=-1)
    response = lkr_sdk.run_inline_query("csv", lkr_query)
    query_results = pd.read_csv(StringIO(response))

    return query_results


def query_cube(query):

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
      query_results = pd.DataFrame(data)

      return query_results

    else:
      response.raise_for_status()


def do_query_results_match(df1, df2):

     # Overwrite the column titles of df2 with those of df1
    df2.columns = df1.columns
    comparison = df1.compare(df2)

    if comparison.empty:
        print("Pass - query results match.")
        return True
    else:
        print("Fail - query results do not match: -")

        # Format the comparison result using tabulate
        comparison.reset_index(inplace=True)  # Reset index to include it in the tabulate output
        print(tabulate(comparison, headers='keys', tablefmt='pretty'))
        return False