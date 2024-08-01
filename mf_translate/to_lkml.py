import re

def entity_to_lkml(mf_entity):

    lkml_dim = {}

    # NAME
    if mf_entity.get("name"):
        lkml_dim["name"] = mf_entity["name"]

    # DESCRIPTION
    if mf_entity.get("description"):
        lkml_dim["description"] = mf_entity["description"]

    # PRIMARY KEY
    if mf_entity.get("type") == 'primary':
        lkml_dim["primary_key"] = True

    # HIDDEN
    lkml_dim["hidden"] = True

    # SQL
    if mf_entity.get("expr"):
        lkml_dim["sql"] = mf_entity["expr"]

    return lkml_dim


def time_granularity_to_timeframes(time_granularity):

    time_granularities = ["day", "week", "month", "quarter", "year"]
    start_index = time_granularities.index(time_granularity)

    timeframes = ["date", "week", "month", "quarter", "year"]
    return timeframes[start_index:]


def dimension_to_lkml(mf_dim):

    lkml_dim = {}

    # NAME
    if mf_dim.get("name"):
        lkml_dim["name"] = mf_dim["name"]

    # DESCRIPTION
    if mf_dim.get("description"):
        lkml_dim["description"] = mf_dim["description"]

    # LABEL
    if mf_dim.get("label"):
        lkml_dim["label"] = mf_dim["label"]

    # SQL
    if mf_dim.get("expr"):
        lkml_dim["sql"] = mf_dim["expr"]

    # TYPE AND TIMEFRAMES
    if mf_dim.get("type") == "categorical":
        lkml_dim["type"] = "string"

    elif mf_dim.get("type") == "time" and mf_dim.get("type_params") and mf_dim["type_params"].get("time_granularity"):
        lkml_dim["type"] = "time"
        lkml_dim["timeframes"] = time_granularity_to_timeframes(mf_dim["type_params"]["time_granularity"])

    elif mf_dim.get("type") == "time":
        lkml_dim["type"] = "date_time"


    return lkml_dim


def filter_to_lkml(mf_filter, mf_models):

    dim_ref_pattern = r"\{\{\s*Dimension\s*\(\s*'([^']+?)'\s*\)\s*\}\}"

    def translate_dim_ref(match):

        dim_inner_ref = match.group(1)                # 'Dimension('delivery__delivery_rating')' -> 'delivery__delivery_rating' 
        entity_name = dim_inner_ref.split("__")[0]    # 'delivery__delivery_rating' -> 'delivery'
        dimension_name = dim_inner_ref.split("__")[1] # 'delivery__delivery_rating' -> 'delivery_rating'

         # Get model for entity
        model_for_entity = None
        for model in mf_models:
            for entity in model["entities"]:
                if entity["name"] == entity_name:
                    model_for_entity = model
                    break

        return "${" + f"{model_for_entity['name']}.{dimension_name}" + "}"

    
    return re.sub(dim_ref_pattern, translate_dim_ref, mf_filter)