import mf_translate.to_cube as to_cube
import logging


def test_filter_expression(monkeypatch):

    deliveries_model= {
        "name": "deliveries",
        "entities": [
            {
                "name": "delivery_id",
                "type": "primary"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model])

    mf_filter = "{{Dimension('delivery_id__delivery_rating')}} = 5"
    cube_filter = to_cube.sql_expression_to_cube(expression=mf_filter,
                                                 from_model=deliveries_model)

    assert cube_filter == "{delivery_rating} = 5"

def test_another_filter_expression(monkeypatch):

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])

    mf_filter = "coalesce( {{ Dimension( 'order_id__discount_code'  ) }}, 'NO_DISCOUNT' ) != 'STAFF_ORDER'"
    cube_filter = to_cube.sql_expression_to_cube(expression=mf_filter,
                                                 from_model=orders_model)

    assert cube_filter == "coalesce( {discount_code}, 'NO_DISCOUNT' ) != 'STAFF_ORDER'"


def test_foreign_filter_expression(monkeypatch):

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ]
    }

    customers_model = {
        "name": "customers",
        "entities": [
            {
                "name": "customer_id",
                "type": "primary"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [customers_model, orders_model])

    mf_filter = "{{ Dimension( 'customer_id__customer_type') }} = 'returning'"
    cube_filter = to_cube.sql_expression_to_cube(expression=mf_filter,
                                                 from_model=orders_model)

    assert cube_filter == "{customers.customer_type} = 'returning'"


def test_calculation_expression(monkeypatch):

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])

    mf_calc = "{{ Dimension('order_id__revenue') }} - {{ Dimension('order_id__discount') }}"
    cube_calc = to_cube.sql_expression_to_cube(expression=mf_calc,
                                               from_model=orders_model)

    assert cube_calc == "{revenue} - {discount}"


def test_unqualified_calculation_expression(monkeypatch):

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "revenue": {},
                "discount": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    mf_calc = "{{ Dimension('order_id__revenue') }} - discount"
    cube_calc = to_cube.sql_expression_to_cube(expression=mf_calc,
                                               from_model=orders_model)

    assert cube_calc == "{revenue} - {CUBE}.discount"


def test_simple_metric(monkeypatch):

    delivery_count = {
        "name": "delivery_count",
        "description": "Metric created from measure delivery_count",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "delivery_count",
                "join_to_timespine": False,
            },
            "expr": "delivery_count"
        },
        "label": "delivery_count"
    }

    deliveries_model = {
        "name": "deliveries",
        "entities": [
            {
                "name": "delivery",
                "type": "primary",
                "expr": "delivery_id"
            }
        ],
        "measures": [
            {
                "name": "delivery_count",
                "agg": "count",
                "create_metric": True,
                "expr": "delivery_id"
            }
        ],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model])

    nodes = {
        "model.jaffle_shop.deliveries": {
            "columns": {
                "delivery_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, "DBT_NODES", nodes)

    cube_delivery_count_measures = to_cube.metric_to_cube_measures(metric=delivery_count,
                                                                   from_model=deliveries_model)

    cube_measure = cube_delivery_count_measures[0]

    assert cube_measure["name"] == "delivery_count"
    assert cube_measure["type"] == "count"
    assert "description" not in cube_measure
    assert "label" not in cube_measure
    assert cube_measure["sql"] == "{CUBE}.delivery_id"
    assert cube_measure["parent_view"] == "deliveries"


def test_simple_metric_with_unsupported_aggregation(monkeypatch, caplog):

    median_delivery_time = {
        "name": "median_delivery_time",
        "description": "Metric created from measure median_delivery_time",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "median_delivery_time",
                "join_to_timespine": False,
            }
        },
        "label": "median_delivery_time"
    }

    deliveries_model = {
        "name": "deliveries",
        "entities": [
            {
                "name": "delivery",
                "type": "primary",
                "expr": "delivery_id"
            }
        ],
        "measures": [
            {
                "name": "median_delivery_time",
                "agg": "median",
                "create_metric": True,
                "expr": "delivery_time_in_mins"
            }
        ],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model])

    nodes = {
        "model.jaffle_shop.deliveries": {
            "columns": {
                "delivery_id": {},
                "delivery_time_in_mins": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, "DBT_NODES", nodes)

    with caplog.at_level(logging.WARNING):
        to_cube.metric_to_cube_measures(metric=median_delivery_time, from_model=deliveries_model)

    assert any(record.levelname == 'WARNING'
                and "median aggregations are not supported." in record.message for record in caplog.records)



def test_another_simple_metric(monkeypatch):

    order_total = {
        "name": "order_total",
        "description": "Sum of total order amonunt. Includes tax + revenue.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_total",
                "join_to_timespine": False
            }
        },
        "label": "Order Total"
    }

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ],
        "measures": [
            {
                "name": "order_total",
                "agg": "sum",
                "description": "The total amount for each order including taxes.",
                "create_metric": True,
                "expr": None
            }
        ],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "order_total": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }


    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    cube_order_total_measures = to_cube.metric_to_cube_measures(metric=order_total,
                                                                from_model=orders_model)

    cube_measure = cube_order_total_measures[0]
    assert cube_measure["name"] == "order_total"
    assert cube_measure["type"] == "sum"
    assert cube_measure["description"] == "Sum of total order amonunt. Includes tax + revenue."
    assert cube_measure["label"] == "Order Total"
    assert cube_measure["sql"] == "{CUBE}.order_total"
    assert cube_measure["parent_view"] == "orders"


def test_metric_with_category_filter(monkeypatch):

    orders_for_returning_customers = {
        "name": "orders_for_returning_customers",
        "description": "Count of orders from returning customers.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_count",
                "join_to_timespine": False
            }
        },
        "filter": {
            "where_filters": [
                {
                    "where_sql_template": "{{ Dimension('customer_id__customer_type') }} = 'returning'"
                }
            ]
        },
        "label": "Returning customer orders"
    }

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            },
            {
                "name": "customer_id",
                "type": "foreign"
            }
        ],
        "measures": [
            {
                "name": "order_count",
                "agg": "sum",
                "create_metric": False,
                "expr": "1"
            }
        ]
    }

    customers_model = {
        "name": "customers",
        "entities": [
            {
                "name": "customer_id",
                "type": "primary",
            }
        ],
        "measures": []
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [customers_model, orders_model])

    cube_large_order_count_measures = to_cube.metric_to_cube_measures(metric=orders_for_returning_customers,
                                                                      from_model=orders_model)

    cube_measure = cube_large_order_count_measures[0]

    assert cube_measure["name"] == "orders_for_returning_customers"
    assert cube_measure["type"] == "sum"
    assert cube_measure["description"] == "Count of orders from returning customers."
    assert cube_measure["label"] == "Returning customer orders"
    assert cube_measure["sql"] == "1"
    assert cube_measure["filters"][0]["sql"] == "{customers.customer_type} = 'returning'"
    assert cube_measure["parent_view"] == "orders"


def test_metric_with_multiple_category_filters(monkeypatch):

    large_order_count = {
        "name": "large_orders",
        "description": "Count of orders with order total over 20. Excludes staff orders.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_count",
                "join_to_timespine": False
            }
        },
        "filter": {
            "where_filters": [
                {
                    "where_sql_template": "{{ Dimension('order_id__is_large_order') }} = true\n"
                },
                {
                    "where_sql_template": "{{ Dimension('order_id__is_staff_order') }} = false\n"
                }
            ]
        },
        "label": "Large Orders"
    }

    orders_model = {
        "name": "orders",
        "entities": [
            {
                "name": "order_id",
                "type": "primary",
                "expr": "order_id"
            }
        ],
        "measures": [
            {
                "name": "order_count",
                "agg": "sum",
                "create_metric": False,
                "expr": "1"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])

    cube_large_order_count = to_cube.metric_to_cube_measures(metric=large_order_count,
                                                             from_model=orders_model)

    cube_measure = cube_large_order_count[0]
    assert cube_measure["name"] == "large_orders"
    assert cube_measure["type"] == "sum"
    assert cube_measure["description"] == "Count of orders with order total over 20. Excludes staff orders."
    assert cube_measure["label"] == "Large Orders"
    assert cube_measure["sql"] == "1"
    assert len(cube_measure["filters"]) == 2
    assert any(filter["sql"] == "{is_large_order} = true" for filter in cube_measure["filters"])
    assert any(filter["sql"] == "{is_staff_order} = false" for filter in cube_measure["filters"])
    assert cube_measure["parent_view"] == "orders"