import os

def add_parentheses_to_sql(sql):

    if len(sql.split()) > 1:
        return f'({sql})'
    else:
        return sql


def set_timezone_for_time_dimension(time_dimension_sql):

    timezone = os.getenv("MF_TRANSLATE_TO_CUBE__TIMEZONE_FOR_TIME_DIMENSIONS")

    if timezone:

        target_database = os.getenv("MF_TRANSLATE__TARGET_DATABASE")

        if target_database == "bigquery":
            return f"TIMESTAMP({time_dimension_sql}, '{timezone}')"
        elif target_database == "snowflake":
            return f"CONVERT_TIMEZONE('{timezone}', {time_dimension_sql})"

    return add_parentheses_to_sql(time_dimension_sql)


def entity_to_cube(mf_entity):

    cube_dim = {}

    # NAME
    if mf_entity.get("name"):
        cube_dim["name"] = mf_entity["name"]

    # DESCRIPTION
    if mf_entity.get("description"):
        cube_dim["description"] = mf_entity["description"]


    # PRIMARY KEY
    if mf_entity.get("type") == 'primary':
        cube_dim["primary_key"] = True


    # PUBLIC
    cube_dim["public"] = False

    # SQL
    if mf_entity.get("expr"):
        cube_dim["sql"] = mf_entity["expr"]
    else:
        cube_dim["sql"] = mf_entity["name"]

    return cube_dim


def dimension_to_cube(mf_dim):

    cube_dim = {}

    # NAME
    if mf_dim.get("name"):
        cube_dim["name"] = mf_dim["name"]

    # DESCRIPTION
    if mf_dim.get("description"):
        cube_dim["description"] = mf_dim["description"]

    # TITLE
    if mf_dim.get("label"):
        cube_dim["title"] = mf_dim["label"]

    # SQL
    if mf_dim.get("expr"):
        if mf_dim.get("type") == "time":
            cube_dim["sql"] = set_timezone_for_time_dimension(mf_dim["expr"])
        else:
            cube_dim["sql"] = add_parentheses_to_sql(mf_dim["expr"])
    else:
        cube_dim["sql"] = mf_dim["name"]

    # TYPE
    if mf_dim.get("type") == "categorical":
        cube_dim["type"] = "string"
    elif mf_dim.get("type") == "time":
        cube_dim["type"] = "time"

    return cube_dim
