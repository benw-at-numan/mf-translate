import re
import logging

SEMANTIC_MODELS = [] # Used by sql_expression_to_lkml(), simple_metric_to_lkml_measure()
METRICS = []         # ""      metric_to_lkml_measures(), model_to_lkml_view()
DBT_NODES = []       # ""      sql_expression_to_lkml()

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


def sql_expression_to_lkml(expression, from_model):
    """
    Translates MetricFlow SQL expression to LookML SQL expression. E.g. '{{ Dimension('delivery__delivery_rating') }}' becomes '${deliveries.delivery_rating}'; revenue * 0.1 becomes ${TABLE}.revenue * 0.1.

    Parameters:
    expression (str): The MetricFlow SQL expression to be translated.
    from_model (dict): The parent MetricFlow model for the expression.

    Returns:
    str: The LookML SQL expression.
    """

    expression = expression.strip()
    expression = expression.replace('\n', ' ')

    # Step 1: Replace unqualified table fields with ${TABLE}.field
    if DBT_NODES:

        node_dict = { node_data['relation_name']: node_data for node_name, node_data in DBT_NODES.items() }
        node_for_model = node_dict[from_model['node_relation']['relation_name']]
        node_columns  = node_for_model['columns'].keys()

        # Pattern to match unqualified fields (words) not in {{ Dimension('...') }}
        unqualified_field_pattern = r"\b(?!\{\{ Dimension\(')\w+\b"

        def translate_unqualified_field(match):
            if match.group(0) in node_columns:
                return f"${{TABLE}}.{match.group(0)}"
            else:
                return match.group(0)

        expression = re.sub(unqualified_field_pattern, translate_unqualified_field, expression)


    # Step 2: Replace {{ Dimension('entity__dimension') }} with ${model.dimension}
    dim_ref_pattern = r"\{\{\s*Dimension\s*\(\s*'([^']+?)'\s*\)\s*\}\}"

    def translate_dim_ref(match):

        dim_inner_ref = match.group(1)                # 'Dimension('delivery__delivery_rating')' -> 'delivery__delivery_rating'
        entity_name = dim_inner_ref.split("__")[0]    # 'delivery__delivery_rating' -> 'delivery'
        dimension_name = dim_inner_ref.split("__")[1] # 'delivery__delivery_rating' -> 'delivery_rating'

         # Get model for entity, dimension pair
        model_for_dimension = None
        for model in SEMANTIC_MODELS:
            for entity in model["entities"]:
                if entity["name"] == entity_name and entity["type"] == 'primary':
                    if 'dimensions' in model:
                        if any(dim['name'] == dimension_name for dim in model['dimensions']):
                            model_for_dimension = model
                            break

        if model_for_dimension['name'] == from_model['name']:
            return "${" + dimension_name + "}"
        else:
            return "${" + f"{model_for_dimension['name']}.{dimension_name}" + "}"


    return re.sub(dim_ref_pattern, translate_dim_ref, expression.strip())

def entity_to_lkml(entity, from_model):
    """
    Translates MetricFlow entity to LookML dimension.

    Parameters:
    entity (dict): The entity to be translated.
    from_model (dict): The parent MetricFlow model for the entity

    Returns:
    dict: The LookML dimension.
    """

    lkml_dim = {}

    lkml_dim["name"] = entity["name"]

    if entity.get("description"):
        lkml_dim["description"] = entity["description"]

    if entity.get("type") == 'primary':
        lkml_dim["primary_key"] = 'yes'

    lkml_dim["hidden"] = 'yes'

    if entity.get("expr"):
        lkml_dim["sql"] = sql_expression_to_lkml(entity["expr"], from_model)

    return lkml_dim


def time_granularity_to_timeframes(time_granularity):
    """
    Helper converting MetricFlow time granularity to list of Looker timeframes.

    Parameters:
    time_granularity (str): E.g. 'day', 'week', 'month', 'quarter', 'year'.

    Returns:
    list: of the Looker timeframes.
    """

    time_granularities = ["day", "week", "month", "quarter", "year"]
    start_index = time_granularities.index(time_granularity)

    timeframes = ["date", "week", "month", "quarter", "year"]
    return timeframes[start_index:]


def dimension_to_lkml(dim, from_model):
    """
    Translates MetricFlow dimension to LookML dimension.

    Parameters:
    dim (dict): The MetricFlow dimension to be translated.
    from_model (dict): The parent MetricFlow model for the dimension.

    Returns:
    dict: The LookML dimension.
    """

    lkml_dim = {}

    if dim.get("name"):
        lkml_dim["name"] = dim["name"]

    if dim.get("description"):
        lkml_dim["description"] = dim["description"]

    if dim.get("label"):
        lkml_dim["label"] = dim["label"]

    if dim.get("expr"):
        lkml_dim["sql"] = sql_expression_to_lkml(dim["expr"], from_model)

    if dim.get("type") == "time" and dim.get("type_params") and dim["type_params"].get("time_granularity"):
        lkml_dim["type"] = "time"
        lkml_dim["timeframes"] = time_granularity_to_timeframes(dim["type_params"]["time_granularity"])

    elif dim.get("type") == "time":
        lkml_dim["type"] = "date_time"

    return lkml_dim


def measure_to_lkml_type(measure, where_filters):
    """
    Translates MetricFlow measure to LookML measure type, count, count_distinct, sum, average, min, max.

    Parameters:
    measure (dict): The MetricFlow measure to be translated.
    where_filters (list): The MetricFlow where filters applied to the measure.

    Returns:
    str: The LookML measure type.
    """

    if measure["agg"] == "count" and len(where_filters) > 0:
        return "count_distinct"
    else:
        return measure["agg"]


def measure_to_lkml_sql(measure, from_model, where_filters):
    """
    Combines a MetricFlow measure with a set of where filters to create a single LookML SQL case when expression.

    Parameters:
    measure (dict): The MetricFlow measure to be translated.
    from_model (dict): The parent MetricFlow model for the measure.
    where_filters (list): The MetricFlow where filters applied to the measure.

    Returns:
    str: The LookML SQL expression.
    """

    if measure["agg"] == "count" and len(where_filters) == 0:
        return None

    measure_expression = measure["expr"] if measure.get("expr") else measure["name"]

    if len(where_filters) == 0:
        sql = sql_expression_to_lkml(measure_expression, from_model)
    else:
        # Incorporate metric's where filters into a SQL `case when` expression. So a filter `{{ Dimension('delivery_id__delivery_rating') }} = 5` becomes `case when (${deliveries.delivery_rating} = 5) then ... end`. An alternative would be to convert where filters to a lkml `filter: [..]`` statement, but this is more trouble than it's worth.
        for index, where_filter in enumerate(where_filters):
            if index == 0:
                sql =  f'case when ({sql_expression_to_lkml(where_filter["where_sql_template"], from_model)})'
            else:
                sql += f'\n               and ({sql_expression_to_lkml(where_filter["where_sql_template"], from_model)})'

        sql +=         f'\n            then ({sql_expression_to_lkml(measure_expression, from_model)})'
        sql +=          '\n         end'

    return sql


def simple_metric_to_lkml_measure(metric, from_model, additional_where_filters=[]):
    """
    Translates a MetricFlow simple metric to a LookML measure.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model for the metric.
    additional_where_filters (list): Optional, any additional MetricFlow where filters to be applied to the metric.

    Returns:
    dict: The LookML measure.
    """

    measures_dict = {}
    for model in SEMANTIC_MODELS:
        for measure in model["measures"]:
            measures_dict[measure["name"]] = measure | {'parent_model': model['name']}

    measure = measures_dict[metric["type_params"]["measure"]["name"]]

    metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])

    lkml_measure = {}
    lkml_measure["name"] = metric["name"]

    lkml_measure["type"] = measure_to_lkml_type(measure, metric_where_filters + additional_where_filters)

    sql = measure_to_lkml_sql(measure, from_model, metric_where_filters + additional_where_filters)
    if sql:
        lkml_measure["sql"] = sql

    lkml_measure["parent_view"] = measure["parent_model"]

    return lkml_measure


def metric_to_lkml_measures(metric, from_model):
    """
    Translates a MetricFlow metric to one or more LookML measures. Currently supports simple and ratio metrics.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model for the metric

    Returns:
    list: A list of LookML measures.
    """

    if metric["type"] in ["conversion", "cumulative", "derived"]:
        # logging.warning(f"Skipped {metric['name']} - {metric['type']} metrics are not yet supported.")
        return []

    elif metric["type"] == "simple":

        lkml_measure = simple_metric_to_lkml_measure(metric, from_model)

        if metric.get("label") != metric['name']:
            lkml_measure["label"] = metric["label"]

        if metric.get("description") != f"Metric created from measure {metric['name']}" \
            and len(metric.get("description")) > 0:
                lkml_measure["description"] = metric["description"]

        return [lkml_measure]

    elif metric["type"] == "ratio":

        metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])
        metrics_dict = {m["name"]: m for m in METRICS}

        # NUMERATOR
        numerator_params = metric["type_params"]["numerator"]
        numerator_where_filters = (numerator_params.get("filter") or  {}).get("where_filters", [])
        numerator_metric = metrics_dict[numerator_params["name"]]

        if numerator_metric.get("type") != "simple":
            # logging.warning(f"Skipped ratio metric {metric['name']} - non-simple numerator metrics are not supported.")
            return []

        lkml_numerator = simple_metric_to_lkml_measure(metric=numerator_metric,
                                                       from_model=from_model,
                                                       additional_where_filters=numerator_where_filters + metric_where_filters)
        lkml_numerator["name"] = f"{metric['name']}_numerator"
        lkml_numerator["hidden"] = 'yes'

        # DENOMINATOR
        denominator_params = metric["type_params"]["denominator"]
        denominator_where_filters = (denominator_params.get("filter") or  {}).get("where_filters", [])
        denominator_metric = metrics_dict[denominator_params["name"]]

        if denominator_metric.get("type") != "simple":
            # logging.warning(f"Skipped ratio metric {metric['name']} - non-simple denominator metrics are not supported.")
            return []

        lkml_denominator = simple_metric_to_lkml_measure(metric=denominator_metric,
                                                         from_model=from_model,
                                                         additional_where_filters=denominator_where_filters + metric_where_filters)
        lkml_denominator["name"] = f"{metric['name']}_denominator"
        lkml_denominator["hidden"] = 'yes'

        # RATIO
        lkml_ratio = {}
        lkml_ratio["name"] = metric["name"]
        lkml_ratio["type"] = "number"

        if metric.get("label"):
            lkml_ratio["label"] = metric["label"]

        if metric.get("description"):
            lkml_ratio["description"] = metric["description"]

        lkml_ratio["sql"] = f"${{{lkml_numerator['name']}}} / nullif(${{{lkml_denominator['name']}}}, 0)"

        if lkml_numerator["parent_view"] != lkml_denominator["parent_view"]:
            # logging.warning(f"Skipped ratio metric {lkml_ratio['name']} - numerator and denominator from different models not supported.")
            return []

        lkml_ratio["parent_view"] = lkml_numerator["parent_view"]

        return [lkml_numerator, lkml_denominator, lkml_ratio]


def model_to_lkml_view(model):
    """
    Translates a MetricFlow model to a LookML view.
    """

    lkml_view = {
        "name": model['name'],
        "sql_table_name": model["node_relation"]["relation_name"],
        "dimension_groups": [],
        "dimensions": [],
        "measures": []
    }

    for entity in model['entities']:
        lkml_dim = entity_to_lkml(entity, model)
        lkml_view['dimensions'].append(lkml_dim)

    for dim in model['dimensions']:
        lkml_dim = dimension_to_lkml(dim, model)

        if lkml_dim.get('type') == 'time':
            lkml_view['dimension_groups'].append(lkml_dim)
        else:
            lkml_view['dimensions'].append(lkml_dim)

    for metric in METRICS:

        lkml_measures = metric_to_lkml_measures(metric, model)

        for lkml_measure in lkml_measures:
            if lkml_measure['parent_view'] == lkml_view['name']:
                del lkml_measure['parent_view']
                lkml_view['measures'].append(lkml_measure)

    return lkml_view