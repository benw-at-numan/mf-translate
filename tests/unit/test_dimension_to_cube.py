import mf_translate.to_cube as to_cube

def test_only_non_null_keys_translated(monkeypatch):

    mf_dim = {
        "name": "delivery_id",
        "description": None,
        "label": None,
        "type": None,
        "expr": None
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "delivery_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    cube_dim = to_cube.dimension_to_cube(dim=mf_dim,
                                         from_model=orders_model)

    assert cube_dim["name"] == "delivery_id"
    assert 'description' not in cube_dim
    assert 'title' not in cube_dim
    assert cube_dim["type"] == "string"
    assert cube_dim["sql"] == "{CUBE}.delivery_id"


def test_category_dimension(monkeypatch):

    delivery_rating = {
        "name": "delivery_rating",
        "description": "The rating the customer gave the delivery person.",
        "label": "Delivery Rating",
        "type": "categorical",
        "is_partition": False,
        "type_params": None,
        "expr": None
    }

    nodes = {
        "model.jaffle_shop.deliveries": {
            "columns": {
                "delivery_rating": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    deliveries_model = {
        "name": "deliveries",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    cube_delivery_rating = to_cube.dimension_to_cube(dim=delivery_rating,
                                                     from_model=deliveries_model)

    assert cube_delivery_rating["name"] == "delivery_rating"
    assert cube_delivery_rating["description"] == "The rating the customer gave the delivery person."
    assert cube_delivery_rating["title"] == "Delivery Rating"
    assert cube_delivery_rating["type"] == "string"
    assert cube_delivery_rating["sql"] == "{CUBE}.delivery_rating"


def test_category_dim_with_expr(monkeypatch):

    is_bulk_transaction = {
        "name": "is_bulk_transaction",
        "type": "categorical",
        "expr": "case when quantity > 10 then true else false end",
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "quantity": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    cube_is_bulk_transaction = to_cube.dimension_to_cube(dim=is_bulk_transaction,
                                                         from_model=orders_model)

    assert cube_is_bulk_transaction["name"] == "is_bulk_transaction"
    assert cube_is_bulk_transaction["type"] == "string"
    assert cube_is_bulk_transaction["sql"] == "(case when {CUBE}.quantity > 10 then true else false end)"


def test_time_dimension_without_timezone(monkeypatch):

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

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "ts_created": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    cube_created_at = to_cube.dimension_to_cube(dim=created_at,
                                                from_model=orders_model)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "{CUBE}.ts_created"


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

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "ts_created": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'BigQuery')
    monkeypatch.setenv('MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(dim=created_at,
                                                from_model=orders_model)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "TIMESTAMP({CUBE}.ts_created, 'America/Los_Angeles')"


def test_time_dimension_with_timezone_2(monkeypatch):

    created_at = {
        "name": "created_at",
        "type": "time",
        "expr": "ts_created",
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "ts_created": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'sNowflake')
    monkeypatch.setenv('MF_TRANSLATE__CUBE_TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(dim=created_at,
                                                from_model=orders_model)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["sql"] == "CONVERT_TIMEZONE('America/Los_Angeles', {CUBE}.ts_created)"