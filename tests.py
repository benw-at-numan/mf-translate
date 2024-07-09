# %%
# IMPORTS
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import subprocess
import os

# %%
# GENERATE METRIC RESULTS

# Set DBT profile
os.environ['DBT_PROFILES_DIR'] = '/Users/benw/.dbt/dbt-slt'

# Define the command to be run
command = ["mf", "query", "--saved-query", "new_customer_orders", "--csv", "logs/simple_metric.csv"]

# Run the command
result = subprocess.run(command, capture_output=True, text=True)

# Check if the command was successful
if result.returncode == 0:
    print("Command executed successfully")
    print("Output:")
    print(result.stdout)
else:
    print("Error occurred while executing command")
    print("Error message:")
    print(result.stderr)


# %%
# UPLOAD RESULTS

# Path to the CSV file
csv_file_path = 'logs/simple_metric.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Add column_1, column_2, etc column headers
num_columns = df.shape[1]
df.columns = [f'column_{i+1}' for i in range(num_columns)]

# Set up the BigQuery client with OAuth authentication
credentials = service_account.Credentials.from_service_account_file(
    'keys/fresh-iridium-428713-j5-fa15322e7990.json'  # Replace with the path to your service account key file
)
project_id = 'fresh-iridium-428713-j5'
client = bigquery.Client(credentials=credentials, project=project_id)

# Define the dataset and table name
dataset_id = 'dbt_slt'
table_id = 'simple_metric'

# Get the full table ID
table_full_id = f"{project_id}.{dataset_id}.{table_id}"

# Define the job configuration for the load job
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

# Load the DataFrame into BigQuery
job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)

# Wait for the load job to complete
job.result()

print(f"Loaded {job.output_rows} rows into {table_full_id}.")
# %%
