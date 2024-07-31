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


def dimension_ref_to_lkml(dimension_reference, semantic_models):

    # Get entity for dimension
    entity_name = dimension_reference.split('__')[0]

    dimension_pattern = r"\{\{\s*Dimension\s*\(\s*'([^']+?)__([^']*?)'\s*\)\s*\}\}"
    match = re.search(dimension_pattern, dimension_reference)
    entity_name = match.group(1)

    # Get model for entity
    entity_model = None
    for model in semantic_models:
        for entity in model["entities"]:
            if entity["name"] == entity_name:
                entity_model = model
                break

    dimension_name = match.group(2)

    return "${" + f"{entity_model['name']}.{dimension_name}" + "}"