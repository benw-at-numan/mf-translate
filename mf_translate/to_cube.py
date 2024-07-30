import os

def set_timezone_for_time_dimension(time_dimension_sql):

    timezone = os.getenv("MF_TRANSLATE_TO_CUBE__TIMEZONE_FOR_TIME_DIMENSIONS")

    if timezone:

        target_database = os.getenv("MF_TRANSLATE__TARGET_DATABASE")

        if target_database == "bigquery":
            return f"TIMESTAMP({time_dimension_sql}, '{timezone}')"
        elif target_database == "snowflake":
            return f"CONVERT_TIMEZONE('{timezone}', {time_dimension_sql})"

    return f'( {time_dimension_sql} )'


def dimension_to_cube(mf_dim):

    cube_dim = {}

    # NAME
    if mf_dim.get("name"):
        cube_dim["name"] = mf_dim["name"]

    # DESCRIPTION
    if mf_dim.get("description") is not None:
        cube_dim["description"] = mf_dim["description"]

    # TITLE
    if mf_dim.get("label"):
        cube_dim["title"] = mf_dim["label"]

    # SQL
    if mf_dim.get("expr"):
        if mf_dim.get("type") == "time":
            cube_dim["sql"] = set_timezone_for_time_dimension(mf_dim["expr"])
        else:
            cube_dim["sql"] = f'( {mf_dim["expr"]} )'

    # TYPE
    if mf_dim.get("type") == "categorical":
        cube_dim["type"] = "string"
    elif mf_dim.get("type") == "time":
        cube_dim["type"] = "time"

    return cube_dim
