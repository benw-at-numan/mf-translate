
def time_granularity_to_timeframes(time_granularity):

    time_granularities = ["day", "week", "month", "quarter", "year"]
    start_index = time_granularities.index(time_granularity)

    timeframes = ["date", "week", "month", "quarter", "year"]
    return timeframes[start_index:]


def dimension_to_lkml(mf_dim):

    lkr_dim = {}

    # NAME
    if mf_dim.get("name"):
        lkr_dim["name"] = mf_dim["name"]

    # DESCRIPTION
    if mf_dim.get("description") is not None:
        lkr_dim["description"] = mf_dim["description"]

    # LABEL
    if mf_dim.get("label"):
        lkr_dim["label"] = mf_dim["label"]

    # SQL
    if mf_dim.get("expr"):
        lkr_dim["sql"] = mf_dim["expr"]

    # TYPE AND TIMEFRAMES
    if mf_dim.get("type") == "categorical":
        lkr_dim["type"] = "string"

    elif mf_dim.get("type") == "time" and mf_dim.get("type_params") and mf_dim["type_params"].get("time_granularity"):
        lkr_dim["type"] = "time"
        lkr_dim["timeframes"] = time_granularity_to_timeframes(mf_dim["type_params"]["time_granularity"])

    elif mf_dim.get("type") == "time":
        lkr_dim["type"] = "date_time"


    return lkr_dim