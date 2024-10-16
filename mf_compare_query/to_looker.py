import os
import pandas as pd
import logging
from tabulate import tabulate

import subprocess

import looker_sdk
from looker_sdk import models40
looker_logger = logging.getLogger('looker_sdk')
looker_logger.setLevel(logging.WARNING)
from io import StringIO

SEMANTIC_MODELS = []
METRICS = []

def set_semantic_manifest(metricflow_semantic_manifest):
    """
    Sets the SEMANTIC_MODELS, METRICS and DBT_NODES globals from the MetricFlow semantic manifest and the DBT manifest.

    Parameters:
    metricflow_semantic_manifest (dict): The MetricFlow semantic manifest.
    """
    global SEMANTIC_MODELS
    global METRICS

    SEMANTIC_MODELS = metricflow_semantic_manifest.get('semantic_models', [])
    METRICS = metricflow_semantic_manifest.get('metrics', [])


def parent_model_for_metrics(metrics):
    """
    Returns the parent semantic model for a given list of metrics. If more than one model is detected, an error is raised.

    Parameters:
    metrics (list): A list of metric names.
    """
   
    parent_model = None

    metrics_dict = {metric['name']: metric for metric in METRICS}

    measures_dict = {}
    for model in SEMANTIC_MODELS:
        for measure in model["measures"]:
            measures_dict[measure["name"]] = measure | {'parent_model': model['name']}

    for metric_name in metrics:
      
        metric = metrics_dict.get(metric_name)

        for input_measure in metric["type_params"]["input_measures"]:
            measure = measures_dict.get(input_measure["name"])
            new_parent_model = measure["parent_model"]
            if parent_model:
                if parent_model != new_parent_model:
                    raise ValueError(f"All query metrics must depend on a single semantic model. Multiple models detected: {parent_model} and {new_parent_model}")
                
            parent_model = new_parent_model

    return parent_model


def field_to_looker_dim(field):
    """
    Converts a MetricFlow field (dimension or entity) to a Looker dimension. For example 'order_id__order_status' becomes 'orders.order_status', 'order_id' becomes 'orders.order_id'.

    Parameters:
    field (str): The MetricFlow field name.

    Returns:
    str: The fully qualified Looker dimension name.
    """

    field_parts = field.split("__")
    entity_name = field_parts[0]

    model_for_entity = None
    for model in SEMANTIC_MODELS:
        for entity in model["entities"]:
            if entity["name"] == entity_name and entity["type"] == 'primary':
                model_for_entity = model
                break

    if len(field_parts) > 1:
        dim = field_parts[1]
        return model_for_entity["name"] + "." + dim
    else:
        return model_for_entity["name"] + "." + entity_name


def query_to_looker_query(metrics, group_by=None, order_by=None, explore=None):
    """
    Converts a MetricFlow query to a Looker query.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).

    Returns:
    looker_sdk.models40.WriteQuery: The Looker query.
    """

    if not os.getenv("MF_TRANSLATE_LOOKER_MODEL"):
        raise ValueError("MF_TRANSLATE_LOOKER_MODEL environment variable must be set.")

    parent_model = parent_model_for_metrics(metrics)

    if not explore:
        explore = parent_model

    lkr_fields = [parent_model + "." + metric for metric in metrics]
    if group_by:
        for field in group_by:
            prefix = '-' if field[0] == '-' else ''
            field = field.replace('-', '')
            lkr_fields.append(prefix + field_to_looker_dim(field))

    if order_by:
        lkr_sorts = []
        metric_names = [metric['name'] for metric in METRICS]
        for field in order_by:
            prefix = '-' if field[0] == '-' else ''
            field = field.replace('-', '')
            if field in metric_names:                       
                lkr_sorts.append(prefix + parent_model + "." + field)
            else:
                lkr_sorts.append(prefix + field_to_looker_dim(field))
    else:
        lkr_sorts = None

    return looker_sdk.models40.WriteQuery(model=os.getenv("MF_TRANSLATE_LOOKER_MODEL"), 
                                          view=explore, 
                                          fields=lkr_fields, 
                                          sorts=lkr_sorts, 
                                          limit=-1)


def query_looker(metrics, group_by=None, order_by=None, explore=None, dev_branch=None):
    """
    Queries Looker for the specified metrics, group by and order by fields.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).

    Returns:
    pandas.DataFrame: The query results.
    """

    lkr_query = query_to_looker_query(metrics, group_by, order_by, explore)
    logging.info(f"Querying Looker explore: {lkr_query.view}, fields: {', '.join(lkr_query.fields)}")
    logging.debug(f"Looker query: {lkr_query}")

    sdk = looker_sdk.init40()

    if dev_branch:
        looker_project = os.getenv("MF_TRANSLATE_LOOKER_PROJECT")
        if not looker_project:
            raise ValueError("MF_TRANSLATE_LOOKER_PROJECT environment variable must be set when using `--to-looker-dev-branch` option.")
        sdk.update_session(models40.WriteApiSession(workspace_id='dev'))
        sdk.update_git_branch(project_id=looker_project, body=models40.WriteGitBranch(name=dev_branch))
        

    response = sdk.run_inline_query("csv", lkr_query)

    query_results_df = pd.read_csv(StringIO(response))

    logging.debug("Looker query results: -")
    logging.debug(tabulate(query_results_df, headers='keys', tablefmt='pretty'))

    return query_results_df


def query_metricflow(metrics, group_by=None, order_by=None, start_time=None, end_time=None):
    """
    Queries MetricFlow for the specified metrics, group by and order by fields. Creates temporarily file `mf_compare_query_results.csv` to store the query results.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).

    Returns:
    pandas.DataFrame: The query results.
    """

    # Define the dbt command
    metrics_list = ','.join(metrics)
    logging.info(f"Querying MetricFlow metrics: {metrics_list}")
    mf_command = f"mf query --metrics {metrics_list} --csv logs/mf_compare_query_results.csv"

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
    result = subprocess.run(mf_command.split(), capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Error occurred while executing command: {result.stderr}")

    # Load the CSV file into a DataFrame
    query_results_df = pd.read_csv(f'logs/mf_compare_query_results.csv', header=None)

    # Add column_1, column_2, etc column headers
    num_columns = query_results_df.shape[1]
    query_results_df.columns = [f'column_{i+1}' for i in range(num_columns)]

    logging.debug("MetricFlow query results: -")
    logging.debug(tabulate(query_results_df, headers='keys', tablefmt='pretty'))

    return query_results_df


def do_query_results_match(df1, df2):
    """
    Compares two DataFrames and prints the comparison results.

    Parameters:
    df1 (pandas.DataFrame): The first DataFrame to compare.
    df2 (pandas.DataFrame): The second DataFrame.

    Returns:
    bool: True if the DataFrames match, False otherwise.
    """

    # Sort the DataFrames by their columns to avoid false negatives due to different row orders
    df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)

    comparison = df1_sorted.compare(df2_sorted)

    if comparison.empty:
        logging.info("PASS: query results match.")
        return True
    else:
        logging.error("FAIL: query results do not match: -")

        comparison.columns = pd.MultiIndex.from_tuples(
            [(col[0], 'MetricFlow' if col[1] == 'self' else 'Looker') for col in comparison.columns]
        )
        comparison.reset_index(inplace=True)  # Reset index to include it in the tabulate output
        logging.info(tabulate(comparison, headers='keys', tablefmt='pretty'))
        return False