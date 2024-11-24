import os
import sys
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


def metric_to_looker_measure(metric_name):
    """
    Converts a MetricFlow metric to a Looker measure. For example 'order_total' becomes 'orders.order_total'.

    Parameters:
    metric (dict): The MetricFlow metric.

    Returns:
    str: The fully qualified Looker measure name.
    """

    parent_model = None

    metrics_dict = {metric['name']: metric for metric in METRICS}

    measures_dict = {}
    for model in SEMANTIC_MODELS:
        for measure in model["measures"]:
            measures_dict[measure["name"]] = measure | {'parent_model': model['name']}


    metric = metrics_dict.get(metric_name)

    for input_measure in metric["type_params"]["input_measures"]:
        measure = measures_dict.get(input_measure["name"])
        new_parent_model = measure["parent_model"]
        if parent_model:
            if parent_model != new_parent_model:
                raise ValueError(f"All query metrics must depend on a single semantic model. Multiple models detected: {parent_model} and {new_parent_model}")

        parent_model = new_parent_model

    return parent_model + "." + metric["name"]


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



    if len(field_parts) > 1:
        dimension_name = field_parts[1]

        # Get model for entity, dimension pair
        model_for_dimension = None
        for model in SEMANTIC_MODELS:
            for entity in model["entities"]:
                if entity["name"] == entity_name and entity["type"] == 'primary':
                    if 'dimensions' in model:
                        if any(dim['name'] == dimension_name for dim in model['dimensions']):
                            model_for_dimension = model
                            break

        return model_for_dimension["name"] + "." + dimension_name
    else:

        # Get (any) model for the entity
        model_for_entity = None
        for model in SEMANTIC_MODELS:
            for entity in model["entities"]:
                if entity["name"] == entity_name and entity["type"] == 'primary':
                    model_for_entity = model
                    break

        return model_for_entity["name"] + "." + entity_name


def query_to_looker_query(explore, metrics, group_by=None, order_by=None):
    """
    Converts a MetricFlow query to a Looker query.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).

    Returns:
    looker_sdk.models40.WriteQuery: The Looker query.
    """

    looker_model = os.getenv("MF_TRANSLATE_LOOKER_MODEL")
    if not looker_model:
        logging.error("Looker LookML model for comparison is not defined.")
        logging.info("Use `export MF_TRANSLATE_LOOKER_MODEL=your_looker_model` to set the model.")
        logging.info("See https://cloud.google.com/looker/docs/lookml-project-files#model_files for more information on Looker models.")
        sys.exit(1)

    lkr_fields = []

    if group_by:
        for field in group_by:
            lkr_fields.append(field_to_looker_dim(field))

    for metric in metrics:
        lkr_fields.append(metric_to_looker_measure(metric))

    if order_by:
        lkr_sorts = []
        metric_names = [metric['name'] for metric in METRICS]
        for field in order_by:
            prefix = '-' if field[0] == '-' else ''
            field = field.replace('-', '')
            if field in metric_names:
                lkr_sorts.append(prefix + metric_to_looker_measure(field))
            else:
                lkr_sorts.append(prefix + field_to_looker_dim(field))
    else:
        lkr_sorts = None

    return looker_sdk.models40.WriteQuery(model=looker_model,
                                          view=explore,
                                          fields=lkr_fields,
                                          sorts=lkr_sorts,
                                          limit=-1)


def query_looker(explore, metrics, group_by=None, order_by=None, dev_branch=None, filters=None):
    """
    Queries Looker for the specified metrics, group by and order by fields.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).
    dev_branch (str): The development git branch to use when querying Looker (optional).
    filters (dict): A dictionary of Looker filters (optional). For example, {'orders.revenue': '>100', 'customers.region': 'US'}.

    Returns:
    pandas.DataFrame: The query results.
    """

    lkr_query = query_to_looker_query(explore, metrics, group_by, order_by)
    
    if filters:
        lkr_query.filters = filters

    logging.info(f"Querying Looker {lkr_query.view} explore fields: {', '.join(lkr_query.fields)}")
    logging.debug(f"Looker query: {lkr_query}")

    sdk = looker_sdk.init40()

    if dev_branch:
        looker_project = os.getenv("MF_TRANSLATE_LOOKER_PROJECT")
        if not looker_project:
            logging.error("Looker project is not defined - this is required when using the `--looker-dev-branch` option.")
            logging.info("Use `export MF_TRANSLATE_LOOKER_PROJECT=your_looker_project` to set the project.")
            logging.info("See https://cloud.google.com/looker/docs/lookml-terms-and-concepts#lookml_project for more information on Looker projects.")
            sys.exit(1)
        sdk.update_session(models40.WriteApiSession(workspace_id='dev'))
        sdk.update_git_branch(project_id=looker_project, body=models40.WriteGitBranch(name=dev_branch))


    response = sdk.run_inline_query("csv", lkr_query)

    query_results_df = pd.read_csv(StringIO(response))

    logging.debug("Looker query results: -")
    logging.debug(tabulate(query_results_df, headers='keys', tablefmt='pretty'))

    return query_results_df


def query_metricflow(metrics, group_by=None, order_by=None, where=None):
    """
    Queries MetricFlow for the specified metrics, group by and order by fields. Creates temporarily file `mf_compare_query_results.csv` to store the query results.

    Parameters:
    metrics (list): A list of metric names.
    group_by (list): A list of dimensions to group by (optional).
    order_by (list): A list of fields to order by (optional).
    where (str): A SQL-like where statement provided as a string and wrapped in quotes (optional).

    Returns:
    pandas.DataFrame: The query results.
    """

    # Define the dbt command
    metrics_list = ','.join(metrics)
    mf_command = [
        "mf", "query",
        "--metrics", f'{metrics_list}',
    ]

    if group_by:
      group_by_list = ','.join(group_by)
      mf_command += ["--group-by", group_by_list]
      logging.info(f"Querying MetricFlow metrics: {metrics_list}, grouped by: {group_by_list}")
    else:
      logging.info(f"Querying MetricFlow metrics: {metrics_list}")

    if order_by:
      order_by_list = ','.join(order_by)
      mf_command += ["--order", order_by_list]

    if where:
        mf_command += ["--where", where]

    mf_command += ["--csv", "logs/mf_compare_query_results.csv"]
    logging.debug(f"Running command: {mf_command}")

    # Delete the results file if it already exists
    if os.path.exists('logs/mf_compare_query_results.csv'):
        os.remove('logs/mf_compare_query_results.csv')

    result = subprocess.run(mf_command, capture_output=True, text=True)
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


def do_query_results_match(metricflow_results, looker_results):
    """
    Compares two DataFrames by comparing all rows and prints the comparison results.

    Parameters:
    metricflow_results (pandas.DataFrame): The first DataFrame to compare.
    looker_results (pandas.DataFrame): The second DataFrame. Columns order and names should match the first DataFrame.

    Returns:
    bool: True if the DataFrames match, False otherwise.
    """

    # Fill NaN values with a consistent placeholder
    metricflow_results_filled = metricflow_results.fillna('<<NULL>>')
    looker_results_filled = looker_results.fillna('<<NULL>>')

    # Convert DataFrames to sets of tuples representing each row
    metricflow_set = set(map(tuple, metricflow_results_filled.to_numpy()))
    looker_set = set(map(tuple, looker_results_filled.to_numpy()))

    # Find matching rows and rows unique to each DataFrame
    matching_rows = metricflow_set & looker_set
    metricflow_only = metricflow_set - looker_set
    looker_only = looker_set - metricflow_set

    # Log the counts
    num_matching_rows = len(matching_rows)
    num_metricflow_only_rows = len(metricflow_only)
    num_looker_only_rows = len(looker_only)

    logging.info(f"Number of matching rows: {num_matching_rows}")
    logging.info(f"Number of rows only in MetricFlow: {num_metricflow_only_rows}")
    logging.info(f"Number of rows only in Looker: {num_looker_only_rows}")

    # Determine if the DataFrames match perfectly
    if num_metricflow_only_rows == 0 and num_looker_only_rows == 0:
        logging.info("PASS: query results match perfectly.")
        return True
    else:
        logging.warning("WARNING: query results do not match exactly.")

        if num_metricflow_only_rows > 0:
            logging.info("Rows only in MetricFlow:")
            for row in metricflow_only:
                logging.info(row)

        if num_looker_only_rows > 0:
            logging.info("Rows only in Looker:")
            for row in looker_only:
                logging.info(row)

        return False