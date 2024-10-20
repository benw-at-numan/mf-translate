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
         # TODO: Need to account for multiple models with the same entity identifier (see sql_expression_to_lkml() in to_looker.py)
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

        ldsh_numerator = simple_metric_to_ldsh_measure(metric=numerator_metric,
                                                       from_model=from_model,
                                                       additional_where_filters=numerator_where_filters + metric_where_filters)
        ldsh_numerator["name"] = f"{metric['name']}_numerator"
        ldsh_numerator["hidden"] = 'yes'

        # DENOMINATOR
        denominator_params = metric["type_params"]["denominator"]
        denominator_where_filters = (denominator_params.get("filter") or  {}).get("where_filters", [])
        denominator_metric = metrics_dict[denominator_params["name"]]

        if denominator_metric.get("type") != "simple":
            logging.warning(f"Skipped ratio metric {metric['name']} - non-simple denominator metrics are not supported.")
            return []

        ldsh_denominator = simple_metric_to_ldsh_measure(metric=denominator_metric,
                                                         from_model=from_model,
                                                         additional_where_filters=denominator_where_filters + metric_where_filters)
        ldsh_denominator["name"] = f"{metric['name']}_denominator"
        ldsh_denominator["hidden"] = 'yes'

        # RATIO
        ldsh_ratio = {}
        ldsh_ratio["name"] = metric["name"]
        ldsh_ratio["type"] = "number"

        if metric.get("label"):
            ldsh_ratio["label"] = metric["label"]

        if metric.get("description"):
            ldsh_ratio["description"] = metric["description"]

        ldsh_ratio["sql"] = f"${{{ldsh_numerator['name']}}} / nullif(${{{ldsh_denominator['name']}}}, 0)"

        if ldsh_numerator["parent_view"] != ldsh_denominator["parent_view"]:
            logging.warning(f"Skipped ratio metric {ldsh_ratio['name']} - numerator and denominator from different models not supported.")
            return []

        ldsh_ratio["parent_view"] = ldsh_numerator["parent_view"]

        logging.info(f"Translated ratio metric {ldsh_ratio['name']}.")
        return [ldsh_numerator, ldsh_denominator, ldsh_ratio]



# ------------------------------------------------------------------------------------------------------------
# The following code is used to merge DBT schema.yml files. This is a helpful utility for working with Lightdash
# as lightdash dimension & metrics have to reside alongside existing DBT model definitions - DBT complains if
# the translated dimension/metrics definitions and the existing model definitions are stored in separate files.
# The solution is to merge the generated output with the existing .yml files using the merge_dbt_yaml() function below.

from ruamel.yaml import YAML
import copy

def merge_dicts(d1, d2):
    for key, value in d2.items():
        if key in d1:
            if isinstance(d1[key], dict) and isinstance(value, dict):
                # Both are dicts, so merge them recursively
                merge_dicts(d1[key], value)
            elif isinstance(d1[key], list) and isinstance(value, list):
                # Both are lists, so merge them intelligently
                d1[key] = merge_lists(d1[key], value)
            else:
                # Overwrite scalar values from d2
                d1[key] = value
        else:
            # Key doesn't exist in d1, so add it
            d1[key] = value

def merge_lists(l1, l2):
    # Check if list items are dicts with 'name' key
    if all(isinstance(item, dict) and 'name' in item for item in l1 + l2):
        # Merge list items based on 'name' key
        merged_list = copy.deepcopy(l1)
        names_in_l1 = {item['name']: item for item in merged_list}
        for item in l2:
            name = item['name']
            if name in names_in_l1:
                merge_dicts(names_in_l1[name], item)
            else:
                merged_list.append(item)
        return merged_list
    else:
        # If items are not dicts with 'name', combine lists without duplicates
        return l1 + [item for item in l2 if item not in l1]

def merge_dbt_yaml(source_yaml_str, update_yaml_str):
    """
    Merges two YAML strings representing dbt models. This works like a left join in SQL in that data from the source
    is only overwritten by data from the update if the key exists in both. Keys are matched in dictionaries by key and
    in lists by the 'name' attribute of the list element.

    Parameters:
        source_yaml_str (str): Original YAML string to be updated.
        update_yaml_str (str): YAML string containing data to merge into the source data.
    """

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 4096  # Prevent line wrapping

    source_yaml = yaml.load(source_yaml_str)
    update_yaml = yaml.load(update_yaml_str)

    # Deep copy the source data and merge the update data into it
    result_data = copy.deepcopy(source_yaml)
    merge_dicts(result_data, update_yaml)

    return result_data