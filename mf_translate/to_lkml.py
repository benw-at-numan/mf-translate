import re

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
    lkml_dim["hidden"] = True

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


def metric_to_lkml(metric, models):

    lkml_metric = {}

    # NAME
    if metric.get("name"):
        lkml_metric["name"] = metric["name"]

    # LABEL
    if metric.get("label"):
        lkml_metric["label"] = metric["label"]

    # DESCRIPTION
    if metric.get("description"):
        lkml_metric["description"] = metric["description"]

    # TYPE
    all_measures =[]
    for model in models:
        all_measures.extend(model["measures"])
    measures_dict = {measure["name"]: measure for measure in all_measures}

    if metric.get("type") == "simple":

        measure_name = metric["type_params"]["measure"]["name"]
        measure = measures_dict[measure_name]

        if metric.get("filter"):
            if metric["filter"].get("where_filters"):
                where_filters = metric["filter"]["where_filters"]
        else:
            where_filters = []

        if measure["agg"] == "count" and len(where_filters) > 0:
            lkml_metric["type"] = "count_distinct"
        else:
            lkml_metric["type"] = measure["agg"]

        # SQL
        if measure["agg"] != "count":

            if len(where_filters) == 0:
                lkml_metric["sql"] = sql_expression_to_lkml(measure['expr'], models)

            else:

                for index, where_filter in enumerate(where_filters):
                    if index == 0:
                        lkml_metric["sql"] =  f'\n    case when ({sql_expression_to_lkml(where_filter["where_sql_template"], models)})'
                    else:
                        lkml_metric["sql"] += f'\n          and ({sql_expression_to_lkml(where_filter["where_sql_template"], models)})'

                lkml_metric["sql"] +=         f'\n        then ({sql_expression_to_lkml(measure["expr"], models)})'
                lkml_metric["sql"] +=          '\n    end'

    return lkml_metric