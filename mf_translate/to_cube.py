import os

def add_parentheses_to_sql(sql):

    if len(sql.split()) > 1:
        return f'({sql})'
    else:
        return sql


def set_timezone_for_time_dimension(time_dimension_sql):

    timezone = os.getenv("MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS")

    if timezone:

        target_database = os.getenv("MF_TRANSLATE__TARGET_DATABASE")

        if target_database == "bigquery":
            return f"TIMESTAMP({time_dimension_sql}, '{timezone}')"
        elif target_database == "snowflake":
            return f"CONVERT_TIMEZONE('{timezone}', {time_dimension_sql})"

    return add_parentheses_to_sql(time_dimension_sql)


def entity_to_cube(entity):

    cube_dim = {}

    # NAME
    if entity.get("name"):
        cube_dim["name"] = entity["name"]

    # DESCRIPTION
    if entity.get("description"):
        cube_dim["description"] = entity["description"]


    # PRIMARY KEY
    if entity.get("type") == 'primary':
        cube_dim["primary_key"] = True


    # PUBLIC
    cube_dim["public"] = False

    # SQL
    if entity.get("expr"):
        cube_dim["sql"] = entity["expr"]
    else:
        cube_dim["sql"] = entity["name"]

    return cube_dim


def dimension_to_cube(dim):

    cube_dim = {}

    # NAME
    if dim.get("name"):
        cube_dim["name"] = dim["name"]

    # DESCRIPTION
    if dim.get("description"):
        cube_dim["description"] = dim["description"]

    # TITLE
    if dim.get("label"):
        cube_dim["title"] = dim["label"]

    # SQL
    if dim.get("expr"):
        if dim.get("type") == "time":
            cube_dim["sql"] = set_timezone_for_time_dimension(dim["expr"])
        else:
            cube_dim["sql"] = add_parentheses_to_sql(dim["expr"])
    else:
        cube_dim["sql"] = dim["name"]

    # TYPE
    if dim.get("type") == "categorical":
        cube_dim["type"] = "string"
    elif dim.get("type") == "time":
        cube_dim["type"] = "time"

    return cube_dim
