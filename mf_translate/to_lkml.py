import re
import logging

def entity_to_lkml(entity):

    lkml_dim = {}

    # NAME
    if entity.get("name"):
        lkml_dim["name"] = entity["name"]

    # DESCRIPTION
    if entity.get("description"):
        lkml_dim["description"] = entity["description"]

    # PRIMARY KEY
    if entity.get("type") == 'primary':
        lkml_dim["primary_key"] = True

    # HIDDEN
    lkml_dim["hidden"] = 'Yes'

    # SQL
    if entity.get("expr"):
        lkml_dim["sql"] = entity["expr"]

    return lkml_dim


def time_granularity_to_timeframes(time_granularity):

    time_granularities = ["day", "week", "month", "quarter", "year"]
    start_index = time_granularities.index(time_granularity)

    timeframes = ["date", "week", "month", "quarter", "year"]
    return timeframes[start_index:]


def dimension_to_lkml(dim):

    lkml_dim = {}

    # NAME
    if dim.get("name"):
        lkml_dim["name"] = dim["name"]

    # DESCRIPTION
    if dim.get("description"):
        lkml_dim["description"] = dim["description"]

    # LABEL
    if dim.get("label"):
        lkml_dim["label"] = dim["label"]

    # SQL
    if dim.get("expr"):
        lkml_dim["sql"] = dim["expr"]

    # TYPE AND TIMEFRAMES
    if dim.get("type") == "categorical":
        lkml_dim["type"] = "string"

    elif dim.get("type") == "time" and dim.get("type_params") and dim["type_params"].get("time_granularity"):
        lkml_dim["type"] = "time"
        lkml_dim["timeframes"] = time_granularity_to_timeframes(dim["type_params"]["time_granularity"])

    elif dim.get("type") == "time":
        lkml_dim["type"] = "date_time"

    return lkml_dim


def sql_expression_to_lkml(expression, models):

    dim_ref_pattern = r"\{\{\s*Dimension\s*\(\s*'([^']+?)'\s*\)\s*\}\}"

    def translate_dim_ref(match):

        dim_inner_ref = match.group(1)                # 'Dimension('delivery__delivery_rating')' -> 'delivery__delivery_rating'
        entity_name = dim_inner_ref.split("__")[0]    # 'delivery__delivery_rating' -> 'delivery'
        dimension_name = dim_inner_ref.split("__")[1] # 'delivery__delivery_rating' -> 'delivery_rating'

         # Get model for entity
        model_for_entity = None
        for model in models:
            for entity in model["entities"]:
                if entity["name"] == entity_name:
                    model_for_entity = model
                    break

        return "${" + f"{model_for_entity['name']}.{dimension_name}" + "}"


    return re.sub(dim_ref_pattern, translate_dim_ref, expression.strip())


def measure_to_lkml_type(measure, where_filters):

    if measure["agg"] == "count" and len(where_filters) > 0:
        return "count_distinct"
    else:
        return measure["agg"]

def measure_to_lkml_sql(measure, where_filters, models):

    if measure["agg"] == "count" and len(where_filters) == 0:
        return None

    measure_expression = measure["expr"] if measure.get("expr") else measure["name"]

    if len(where_filters) == 0:
        sql = sql_expression_to_lkml(measure_expression, models)
    else:
        # Incorporate metric's where filters into a SQL `case when` expression. So a filter `{{ Dimension('delivery_id__delivery_rating') }} = 5` becomes `case when (${deliveries.delivery_rating} = 5) then ... end`. An alternative would be to convert where filters to a lkml `filter: [..]`` statement, but this is more trouble than it's worth.
        for index, where_filter in enumerate(where_filters):
            if index == 0:
                sql =  f'\n    case when ({sql_expression_to_lkml(where_filter["where_sql_template"], models)})'
            else:
                sql += f'\n          and ({sql_expression_to_lkml(where_filter["where_sql_template"], models)})'

        sql +=         f'\n        then ({sql_expression_to_lkml(measure_expression, models)})'
        sql +=          '\n    end'

    return sql


def simple_metric_to_lkml_measure(metric, models, additional_where_filters=[]):

    measures_dict = {measure["name"]: measure for model in models for measure in model["measures"]}
    measure = measures_dict[metric["type_params"]["measure"]["name"]]

    metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])

    lkml_measure = {}
    lkml_measure["name"] = metric["name"]

    lkml_measure["type"] = measure_to_lkml_type(measure, metric_where_filters + additional_where_filters)

    sql = measure_to_lkml_sql(measure, metric_where_filters + additional_where_filters, models)
    if sql:
        lkml_measure["sql"] = sql

    return lkml_measure


def metric_to_lkml_measures(metric, models, parent_metrics=[]):

    parent_metrics_dict = {m["name"]: m for m in parent_metrics}

    if metric["type"] in ["conversion", "cumulative", "derived"]:
        logging.warning(f"Skipped {metric['name']} - {metric['type']} metrics are not yet supported.")
        return []

    elif metric.get("type") == "simple":

        lkml_measure = simple_metric_to_lkml_measure(metric, models)

        if metric.get("label"):
            lkml_measure["label"] = metric["label"]

        if metric.get("description"):
            lkml_measure["description"] = metric["description"]

        logging.info(f"Translated simple metric {lkml_measure['name']}.") 
        return [lkml_measure]

    elif metric.get("type") == "ratio":

        metric_where_filters = (metric.get("filter") or  {}).get("where_filters", [])

        # NUMERATOR
        numerator_params = metric["type_params"]["numerator"]
        numerator_where_filters = (numerator_params.get("filter") or  {}).get("where_filters", [])
        numerator_metric = parent_metrics_dict[numerator_params["name"]]

        if numerator_metric.get("type") != "simple":
            logging.warning(f"Ratio metric {metric['name']} has a non-simple numerator metric: {numerator_metric['name']}. This is not supported.")
            return []

        lkml_numerator = simple_metric_to_lkml_measure(metric=numerator_metric,
                                                       models=models,
                                                       additional_where_filters=numerator_where_filters + metric_where_filters)       
        lkml_numerator["name"] = f"{metric['name']}_numerator"
        lkml_numerator["hidden"] = 'Yes'

        # DENOMINATOR
        denominator_params = metric["type_params"]["denominator"]
        denominator_where_filters = (denominator_params.get("filter") or  {}).get("where_filters", [])
        denominator_metric = parent_metrics_dict[denominator_params["name"]]

        if denominator_metric.get("type") != "simple":
            logging.warning(f"Ratio metric {metric['name']} has a non-simple denominator metric: {denominator_metric['name']}. This is not supported.")
            return []

        lkml_denominator = simple_metric_to_lkml_measure(metric=denominator_metric,
                                                         models=models,
                                                         additional_where_filters=denominator_where_filters + metric_where_filters)
        lkml_denominator["name"] = f"{metric['name']}_denominator"
        lkml_denominator["hidden"] = 'Yes'

        # RATIO
        lkml_ratio = {}
        lkml_ratio["name"] = metric["name"]
        lkml_ratio["type"] = "number"

        if metric.get("label"):
            lkml_ratio["label"] = metric["label"]

        if metric.get("description"):
            lkml_ratio["description"] = metric["description"]

        lkml_ratio["sql"] = f"${{{lkml_numerator['name']}}} / nullif(${{{lkml_denominator['name']}}}, 0)"

        logging.info(f"Translated ratio metric {lkml_ratio['name']}.")
        return [lkml_numerator, lkml_denominator, lkml_ratio]