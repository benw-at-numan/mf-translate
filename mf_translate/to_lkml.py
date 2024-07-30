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
    if mf_dim.get("description") is not None:
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