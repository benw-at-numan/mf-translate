import mf_translate.to_cube as to_cube

def test_only_non_null_keys_translated():

    dimension = {
        "name": "delivery_id",
        "description": None,
        "label": None,
        "type": None,
        "expr": None
    }

    cube_dimension = to_cube.dimension_to_cube(dimension)

    assert cube_dimension["name"] == "delivery_id"
    assert 'description' not in cube_dimension
    assert 'title' not in cube_dimension
    assert 'type' not in cube_dimension
    assert cube_dimension["sql"] == "delivery_id"


def test_category_dimension():

    delivery_rating = {
        "name": "delivery_rating",
        "description": "The rating the customer gave the delivery person.",
        "label": "Delivery Rating",
        "type": "categorical",
        "is_partition": False,
        "type_params": None,
        "expr": None
    }

    cube_delivery_rating = to_cube.dimension_to_cube(delivery_rating)

    assert cube_delivery_rating["name"] == "delivery_rating"
    assert cube_delivery_rating["description"] == "The rating the customer gave the delivery person."
    assert cube_delivery_rating["title"] == "Delivery Rating"
    assert cube_delivery_rating["type"] == "string"
    assert cube_delivery_rating["sql"] == "delivery_rating"


def test_category_dim_with_expr():

    is_bulk_transaction = {
        "name": "is_bulk_transaction",
        "type": "categorical",
        "expr": "case when quantity > 10 then true else false end",
    }

    cube_is_bulk_transaction = to_cube.dimension_to_cube(is_bulk_transaction)

    assert cube_is_bulk_transaction["name"] == "is_bulk_transaction"
    assert cube_is_bulk_transaction["type"] == "string"
    assert cube_is_bulk_transaction["sql"] == "(case when quantity > 10 then true else false end)"


def test_time_dimension_without_timezone():

    created_at = {
        "name": "created_at",
        "type": "time",
        "label": "Time of creation",
        "description": "Time of creation, without timezone",
        "expr": "ts_created",
        "type_params": {
            "time_granularity": "day"
        }
    }

    cube_created_at = to_cube.dimension_to_cube(created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "ts_created"


def test_time_dimension_with_timezone(monkeypatch):

    created_at = {
        "name": "created_at",
        "type": "time",
        "label": "Time of creation",
        "description": "Time of creation, without timezone",
        "expr": "ts_created",
        "type_params": {
            "time_granularity": "day"
        }
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'bigquery')
    monkeypatch.setenv('MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "TIMESTAMP(ts_created, 'America/Los_Angeles')"


def test_time_dimension_with_timezone_2(monkeypatch):

    created_at = {
        "name": "created_at",
        "type": "time",
        "expr": "ts_created",
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'snowflake')
    monkeypatch.setenv('MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["sql"] == "CONVERT_TIMEZONE('America/Los_Angeles', ts_created)"