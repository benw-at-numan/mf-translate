import re
import os
import logging

SEMANTIC_MODELS = [] # Used in sql_expression_to_cube()
METRICS = []
DBT_NODES = []

def set_manifests(metricflow_semantic_manifest, dbt_manifest):
    """
    Sets the SEMANTIC_MODELS, METRICS and DBT_NODES globals from the MetricFlow semantic manifest and the DBT manifest.

    Parameters:
    metricflow_semantic_manifest (dict): The MetricFlow semantic manifest.
    dbt_manifest (dict): The DBT manifest.
    """
    global SEMANTIC_MODELS
    global METRICS
    global DBT_NODES

    SEMANTIC_MODELS = metricflow_semantic_manifest.get('semantic_models', [])
    METRICS = metricflow_semantic_manifest.get('metrics', [])
    DBT_NODES = dbt_manifest.get('nodes', [])


def sql_expression_to_cube(expression, from_model):
    """
    Translates MetricFlow SQL expression to Cube SQL expression. E.g. '{{ Dimension('delivery_id__delivery_rating') }}' becomes '{deliveries.delivery_rating}'; revenue * 0.1 becomes {CUBE}.revenue * 0.1.

    Parameters:
    expression (str): The MetricFlow SQL expression to be translated.
    from_model (dict): The parent MetricFlow model which the expression belongs to.

    Returns:
    str: The Cube SQL expression.
    """

    # Step 1: Replace unqualified table fields with {CUBE}.field
    if DBT_NODES:

        node_dict = { node_data['relation_name']: node_data for node_name, node_data in DBT_NODES.items() }
        node_for_model = node_dict[from_model['node_relation']['relation_name']]
        node_columns  = node_for_model['columns'].keys()

        # Pattern to match unqualified fields (words) not in {{ Dimension('...') }}
        unqualified_field_pattern = r"\b(?!\{\{ Dimension\(')\w+\b"

        def translate_unqualified_field(match):
            if match.group(0) in node_columns:
                return f"{{CUBE}}.{match.group(0)}"
            else:
                return match.group(0)

        expression = re.sub(unqualified_field_pattern, translate_unqualified_field, expression)


    # Step 2: Replace {{ Dimension('entity__dimension') }} with {model.dimension}
    dim_ref_pattern = r"\{\{\s*Dimension\s*\(\s*'([^']+?)'\s*\)\s*\}\}"

    def translate_dim_ref(match):

        dim_inner_ref = match.group(1)                # 'Dimension('delivery__delivery_rating')' -> 'delivery__delivery_rating'
        entity_name = dim_inner_ref.split("__")[0]    # 'delivery__delivery_rating' -> 'delivery'
        dimension_name = dim_inner_ref.split("__")[1] # 'delivery__delivery_rating' -> 'delivery_rating'

         # Get model for entity
        model_for_entity = None
        for model in SEMANTIC_MODELS:
            for entity in model["entities"]:
                if entity["name"] == entity_name and entity["type"] == 'primary':
                    model_for_entity = model
                    break

        if model_for_entity['name'] == from_model['name']:
            return "{" + dimension_name + "}"
        else:
            return "{" + f"{model_for_entity['name']}.{dimension_name}" + "}"

    return re.sub(dim_ref_pattern, translate_dim_ref, expression.strip())


def entity_to_cube(entity, from_model):
    """
    Translates a MetricFlow entity to a Cube dimension.

    Parameters:
    entity (dict): The MetricFlow entity to be translated.
    from_model (dict): The parent MetricFlow model for the entity.

    Returns:
    dict: The Cube dimension.
    """

    cube_dim = {}

    if entity.get("name"):
        cube_dim["name"] = entity["name"]

    if entity.get("description"):
        cube_dim["description"] = entity["description"]

    if entity.get("type") == 'primary':
        cube_dim["primary_key"] = True

    cube_dim["type"] = "string"

    cube_dim["public"] = False

    cube_dim["sql"] = sql_expression_to_cube(entity.get("expr") or entity["name"],
                                             from_model)
    return cube_dim


def add_parentheses_to_sql(sql):
    """
    Helper function for adding parentheses to an SQL expression if it is more than one word. Cube generates invalid SQL for dimension expressions with more than one word if they are not enclosed in parentheses.
    """

    if len(sql.split()) > 1:
        return f'({sql})'
    else:
        return sql


def set_timezone_for_time_dimension(time_dimension_sql):
    """
    Helper function for converting a time dimension SQL expression to a timezone-aware SQL expression. The timezone is set by the MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS environment variable.

    Cube requires a timestamp whereas DBT wants a datetime:
         - https://cube.dev/docs/guides/recipes/data-modeling/string-time-dimensions
         - https://github.com/dbt-labs/metricflow/issues/733
    """

    timezone = os.getenv("MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS")

    if timezone:

        target_database = os.getenv("MF_TRANSLATE__TARGET_DATABASE")

        if target_database.lower() == "bigquery":
            return f"TIMESTAMP({time_dimension_sql}, '{timezone}')"
        elif target_database.lower() == "snowflake":
            return f"CONVERT_TIMEZONE('{timezone}', {time_dimension_sql})"

    return add_parentheses_to_sql(time_dimension_sql)


def dimension_to_cube(dim, from_model):
    """
    Translates a MetricFlow dimension to a Cube dimension.

    Parameters:
    dim (dict): The MetricFlow dimension to be translated.
    from_model (dict): The parent MetricFlow model for the dimension

    Returns:
    dict: The Cube dimension.
    """

    cube_dim = {}

    cube_dim["name"] = dim["name"]

    if dim.get("description"):
        cube_dim["description"] = dim["description"]

    if dim.get("label"):
        cube_dim["title"] = dim["label"]

    cube_expr = sql_expression_to_cube(dim.get("expr") or dim["name"], from_model)

    if dim.get("type") == "time":
        cube_dim["sql"] = set_timezone_for_time_dimension(cube_expr)
    else:
        cube_dim["sql"] = add_parentheses_to_sql(cube_expr)

    if dim.get("type") == "time":
        cube_dim["type"] = "time"
    else:
        cube_dim["type"] = "string"

    return cube_dim


def simple_metric_to_cube_measure(metric, from_model, additional_where_filters=[]):
    """
    Translates a MetricFlow simple metric to a Cube measure.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model for the metric.
    additional_where_filters (list): Optional, any additional MetricFlow where filters to be applied to the metric.

    Returns:
    dict: The Cube measure.
    """

    measures_dict = {}
    for model in SEMANTIC_MODELS:
        for measure in model["measures"]:
            measures_dict[measure["name"]] = measure | {'parent_model': model['name']}

    measure = measures_dict[metric["type_params"]["measure"]["name"]]

    cube_measure = {}
    cube_measure["name"] = metric["name"]

    if measure["agg"] in ['count', 'count_distinct', 'sum', 'min', 'max']:
        cube_measure["type"] = measure["agg"]
    elif measure['agg'] == 'average':
        cube_measure["type"] = 'avg'
    else:
        logging.warning(f"Skipped {metric['name']} - {measure['agg']} aggregations are not supported.")
        return None

    sql = sql_expression_to_cube(measure.get("expr") or measure["name"], from_model)
    if sql:
        cube_measure["sql"] = sql

    metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])

    if metric_where_filters or additional_where_filters:
        cube_measure["filters"] = []
        for where_filter in (metric_where_filters + additional_where_filters):
            cube_measure["filters"].append({"sql": sql_expression_to_cube(where_filter["where_sql_template"], from_model)})

    cube_measure["parent_view"] = measure["parent_model"]

    return cube_measure


def metric_to_cube_measures(metric, from_model):
    """
    Translates a MetricFlow metric to one or more Cube measures. Currently supports simple and ratio metrics.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model which the metric belongs to.

    Returns:
    list: A list of Cube measures.
    """

    if metric["type"] in ["conversion", "cumulative", "derived"]:
        logging.warning(f"Skipped {metric['name']} - {metric['type']} metrics are not yet supported.")
        return []

    elif metric["type"] == "simple":

        cube_measure = simple_metric_to_cube_measure(metric, from_model)
        if not cube_measure:
            return []

        if metric.get("label") != metric['name']:
            cube_measure["title"] = metric["label"]

        if metric.get("description") != f"Metric created from measure {metric['name']}":
            cube_measure["description"] = metric["description"]

        logging.info(f"Translated simple metric {cube_measure['name']}.")
        return [cube_measure]

    elif metric["type"] == "ratio":

        metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])
        metrics_dict = {m["name"]: m for m in METRICS}

        # NUMERATOR
        numerator_params = metric["type_params"]["numerator"]
        numerator_where_filters = (numerator_params.get("filter") or  {}).get("where_filters", [])
        numerator_metric = metrics_dict[numerator_params["name"]]

        if numerator_metric.get("type") != "simple":
            logging.warning(f"Skipped ratio metric {metric['name']} - non-simple numerator metrics are not supported.")
            return []

        cube_numerator = simple_metric_to_cube_measure(metric=numerator_metric,
                                                       from_model=from_model,
                                                       additional_where_filters=numerator_where_filters + metric_where_filters)
        cube_numerator["name"] = f"{metric['name']}_numerator"
        cube_numerator["public"] = False

        # DENOMINATOR
        denominator_params = metric["type_params"]["denominator"]
        denominator_where_filters = (denominator_params.get("filter") or  {}).get("where_filters", [])
        denominator_metric = metrics_dict[denominator_params["name"]]

        if denominator_metric.get("type") != "simple":
            logging.warning(f"Skipped ratio metric {metric['name']} - non-simple denominator metrics are not supported.")
            return []

        cube_denominator = simple_metric_to_cube_measure(metric=denominator_metric,
                                                         from_model=from_model,
                                                         additional_where_filters=denominator_where_filters + metric_where_filters)
        cube_denominator["name"] = f"{metric['name']}_denominator"
        cube_denominator["public"] = False

        # RATIO
        cube_ratio = {}
        cube_ratio["name"] = metric["name"]
        cube_ratio["type"] = "number"

        if metric.get("label"):
            cube_ratio["title"] = metric["label"]

        if metric.get("description"):
            cube_ratio["description"] = metric["description"]

        cube_ratio["sql"] = f"{{{cube_numerator['name']}}} / nullif({{{cube_denominator['name']}}}, 0)"

        if cube_numerator["parent_view"] != cube_denominator["parent_view"]:
            logging.warning(f"Skipped ratio metric {cube_ratio['name']} - numerator and denominator from different models not supported.")
            return []

        cube_ratio["parent_view"] = cube_numerator["parent_view"]

        logging.info(f"Translated ratio metric {cube_ratio['name']}.")
        return [cube_numerator, cube_denominator, cube_ratio]


def model_to_cube_cube(model):
    """
    Translates a MetricFlow model to a Cube.dev cube.
    """

    cube = {
        "name": model['name'],
        "sql_table": model["node_relation"]["relation_name"],
        "dimensions": [],
        "measures": []
    }

    for entity in model['entities']:
        cube_dim = entity_to_cube(entity, model)
        cube['dimensions'].append(cube_dim)

    for dim in model['dimensions']:
        cube_dim = dimension_to_cube(dim, model)
        cube['dimensions'].append(cube_dim)

    for metric in METRICS:

        cube_measures = metric_to_cube_measures(metric, model)

        for cube_measure in cube_measures:
            if cube_measure['parent_view'] == cube['name']:
                del cube_measure['parent_view']
                cube['measures'].append(cube_measure)

    return cube