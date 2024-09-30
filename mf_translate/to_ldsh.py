import re
import logging

SEMANTIC_MODELS = [] # Used by simple_metric_to_ldsh_measure()
METRICS = []         # ""      metric_to_ldsh_measures(), model_to_ldsh_view()
DBT_NODES = []       # ""      sql_expression_to_ldsh()

def sql_expression_to_ldsh(expression, from_model):
    """
    Translates MetricFlow SQL expression to Lightdash SQL expression. E.g. '{{ Dimension('delivery__delivery_rating') }}' becomes '${deliveries.delivery_rating}'; revenue * 0.1 becomes ${TABLE}.revenue * 0.1.

    Parameters:
    expression (str): The MetricFlow SQL expression to be translated.
    from_model (dict): The parent MetricFlow model for the expression.

    Returns:
    str: The Lightdash SQL expression.
    """

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

         # Get model for entity
        model_for_entity = None
        for model in SEMANTIC_MODELS:
            for entity in model["entities"]:
                if entity["name"] == entity_name and entity["type"] == 'primary':
                    model_for_entity = model
                    break

        if model_for_entity['name'] == from_model['name']:
            return "${" + dimension_name + "}"
        else:
            return "${" + f"{model_for_entity['name']}.{dimension_name}" + "}"


    return re.sub(dim_ref_pattern, translate_dim_ref, expression.strip())


def measure_to_ldsh_sql(measure, from_model, where_filters):
    """
    Combines a MetricFlow measure with a set of where filters to create a single Lightdash SQL case when expression.

    Parameters:
    measure (dict): The MetricFlow measure to be translated.
    from_model (dict): The parent MetricFlow model for the measure.
    where_filters (list): The MetricFlow where filters applied to the measure.

    Returns:
    str: The Lightdash SQL expression.
    """

    measure_expression = measure["expr"] if measure.get("expr") else measure["name"]

    if len(where_filters) == 0:
        sql = sql_expression_to_ldsh(measure_expression, from_model)
    else:
        # Incorporate metric's where filters into a SQL `case when` expression. So a filter `{{ Dimension('delivery_id__delivery_rating') }} = 5` becomes `case when (${deliveries.delivery_rating} = 5) then ... end`. An alternative would be to convert where filters to a ldsh `filter: [..]`` statement, but this is more trouble than it's worth.
        for index, where_filter in enumerate(where_filters):
            if index == 0:
                sql =  f'case when ({sql_expression_to_ldsh(where_filter["where_sql_template"], from_model)})'
            else:
                sql += f'\n               and ({sql_expression_to_ldsh(where_filter["where_sql_template"], from_model)})'

        sql +=         f'\n            then ({sql_expression_to_ldsh(measure_expression, from_model)})'
        sql +=          '\n         end'

    return sql


def simple_metric_to_ldsh_measure(metric, from_model, additional_where_filters=[]):
    """
    Translates a MetricFlow simple metric to a Lightdash measure.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model for the metric.
    additional_where_filters (list): Optional, any additional MetricFlow where filters to be applied to the metric.

    Returns:
    dict: The Lightdash measure.
    """

    measures_dict = {}
    for model in SEMANTIC_MODELS:
        for measure in model["measures"]:
            measures_dict[measure["name"]] = measure | {'parent_model': model['name']}

    measure = measures_dict[metric["type_params"]["measure"]["name"]]

    metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])

    ldsh_measure = {}
    ldsh_measure["name"] = metric["name"]
    ldsh_measure["type"] = measure["agg"]

    sql = measure_to_ldsh_sql(measure, from_model, metric_where_filters + additional_where_filters)
    if sql:
        ldsh_measure["sql"] = sql

    ldsh_measure["parent_view"] = measure["parent_model"]

    return ldsh_measure


def metric_to_ldsh_measures(metric, from_model):
    """
    Translates a MetricFlow metric to one or more Lightdash measures. Currently supports simple and ratio metrics.

    Parameters:
    metric (dict): The MetricFlow metric to be translated.
    from_model (dict): The parent MetricFlow model for the metric

    Returns:
    list: A list of Lightdash measures.
    """

    if metric["type"] in ["conversion", "cumulative", "derived"]:
        logging.warning(f"Skipped {metric['name']} - {metric['type']} metrics are not yet supported.")
        return []

    elif metric["type"] == "simple":

        ldsh_measure = simple_metric_to_ldsh_measure(metric, from_model)

        if metric.get("label") != metric['name']:
            ldsh_measure["label"] = metric["label"]

        if metric.get("description") != f"Metric created from measure {metric['name']}":
            ldsh_measure["description"] = metric["description"]

        logging.info(f"Translated simple metric {ldsh_measure['name']}.")
        return [ldsh_measure]