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
    assert "title" not in cube_measure
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

    with caplog.at_level(logging.DEBUG):
        cube_measures = to_cube.metric_to_cube_measures(metric=median_delivery_time, from_model=deliveries_model)

    assert any(record.levelname == 'DEBUG'
                and "median aggregations are not supported." in record.message for record in caplog.records)

    assert len(cube_measures) == 0


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
    assert cube_measure["title"] == "Order Total"
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
    assert cube_measure["title"] == "Returning customer orders"
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
    assert cube_measure["title"] == "Large Orders"
    assert cube_measure["sql"] == "1"
    assert len(cube_measure["filters"]) == 2
    assert any(filter["sql"] == "{is_large_order} = true" for filter in cube_measure["filters"])
    assert any(filter["sql"] == "{is_staff_order} = false" for filter in cube_measure["filters"])
    assert cube_measure["parent_view"] == "orders"


def test_ratio_metric(monkeypatch):

    food_revenue_pct = {
        "name": "food_revenue_pct",
        "description": "The % of order revenue from food.",
        "type": "ratio",
        "type_params": {
            "numerator": {
                "name": "food_revenue"
            },
            "denominator": {
                "name": "revenue"
            }
        },
        "label": "Food Revenue %"
    }

    food_revenue = {
        "name": "food_revenue",
        "description": "The revenue from food in each order",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "food_revenue_measure",
                "join_to_timespine": False
            }
        },
        "label": "Food Revenue"
    }

    revenue = {
        "name": "revenue",
        "description": "Sum of the product revenue for each order item. Excludes tax.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "revenue_measure",
                "join_to_timespine": False,
                "fill_nulls_with": 0
            }
        },
        "label": "Revenue"
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
                "name": "revenue_measure",
                "agg": "sum",
                "description": "The revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each product in an order.",
                "create_metric": False,
                "expr": "product_price"
            },
            {
                "name": "food_revenue_measure",
                "agg": "sum",
                "description": "The food revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each food product in an order.",
                "create_metric": False,
                "expr": "case when is_food_item = 1 then product_price else 0 end"
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
                "is_food_item": {},
                "product_price": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_cube, "METRICS", [food_revenue, revenue, food_revenue_pct])
    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    cube_food_revenue_pct_measures = to_cube.metric_to_cube_measures(metric=food_revenue_pct,
                                                                     from_model=orders_model)

    cube_numerator = cube_food_revenue_pct_measures[0]
    assert cube_numerator["name"] == "food_revenue_pct_numerator"
    assert cube_numerator["public"] == False
    assert cube_numerator["type"] == "sum"
    assert 'description' not in cube_numerator
    assert 'title' not in cube_numerator
    assert cube_numerator["sql"] == "case when {CUBE}.is_food_item = 1 then {CUBE}.product_price else 0 end"
    assert cube_numerator["parent_view"] == "orders"

    cube_denominator = cube_food_revenue_pct_measures[1]
    assert cube_denominator["name"] == "food_revenue_pct_denominator"
    assert cube_denominator["public"] == False
    assert cube_denominator["type"] == "sum"
    assert 'description' not in cube_denominator
    assert 'title' not in cube_denominator
    assert cube_denominator["sql"] == "{CUBE}.product_price"
    assert cube_denominator["parent_view"] == "orders"

    cube_ratio = cube_food_revenue_pct_measures[2]
    assert cube_ratio["name"] == "food_revenue_pct"
    assert cube_ratio["type"] == "number"
    assert cube_ratio["description"] == "The % of order revenue from food."
    assert cube_ratio["title"] == "Food Revenue %"
    assert cube_ratio["sql"] == "{food_revenue_pct_numerator} / nullif({food_revenue_pct_denominator}, 0)"
    assert cube_ratio["parent_view"] == "orders"


def test_filtered_ratio_metric(monkeypatch):

    pc_deliveries_with_5_stars = {
        "name": "pc_deliveries_with_5_stars",
        "description": "Percentage of deliveries that received a 5-star rating.",
        "type": "ratio",
        "type_params": {
            "numerator": {
                "name": "delivery_count",
                "filter": {
                    "where_filters": [
                        {
                            "where_sql_template": "{{ Dimension('delivery__delivery_rating') }} = 5\n"
                        }
                    ]
                },
                "alias": "five_star_deliveries_count"
            },
            "denominator": {
                "name": "delivery_count",
            }
        },
        "filter": {
            "where_filters": [
                {
                    "where_sql_template": "coalesce({{ Dimension('order_id__discount_code') }}, 'NO_DISCOUNT') != 'STAFF_ORDER'"
                }
            ]
        },
        "label": "Deliveries with 5 stars (%)"
    }

    delivery_count = {
        "name": "delivery_count",
        "description": "Metric created from measure delivery_count",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "delivery_count_measure",
                "join_to_timespine": False
            }
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
                "name": "delivery_count_measure",
                "agg": "count",
                "create_metric": False,
                "expr": "delivery_id"
            }
        ],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
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
        "measures": [],
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    nodes = {
        "model.jaffle_shop.deliveries": {
            "columns": {
                "delivery_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model, orders_model])
    monkeypatch.setattr(to_cube, "METRICS", [pc_deliveries_with_5_stars, delivery_count])
    monkeypatch.setattr(to_cube, "DBT_NODES", nodes)

    cube_pc_deliveries_with_5_stars = to_cube.metric_to_cube_measures(metric=pc_deliveries_with_5_stars,
                                                                      from_model=deliveries_model)

    cube_numerator = cube_pc_deliveries_with_5_stars[0]
    assert cube_numerator["name"] == "pc_deliveries_with_5_stars_numerator"
    assert cube_numerator["public"] == False
    assert cube_numerator["type"] == "count"
    assert 'description' not in cube_numerator
    assert 'title' not in cube_numerator
    assert cube_numerator["sql"] == "{CUBE}.delivery_id"
    assert len(cube_numerator["filters"]) == 2
    assert any(filter["sql"] == "{delivery_rating} = 5" for filter in cube_numerator["filters"])
    assert any(filter["sql"] == "coalesce({orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER'" for filter in cube_numerator["filters"])
    assert cube_numerator["parent_view"] == "deliveries"

    cube_denominator = cube_pc_deliveries_with_5_stars[1]
    assert cube_denominator["name"] == "pc_deliveries_with_5_stars_denominator"
    assert cube_denominator["public"] == False
    assert cube_denominator["type"] == "count"
    assert 'description' not in cube_denominator
    assert 'title' not in cube_denominator
    assert cube_denominator["sql"] == "{CUBE}.delivery_id"
    assert len(cube_denominator["filters"]) == 1
    assert cube_denominator["filters"][0]["sql"] == "coalesce({orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER'"
    assert cube_denominator["parent_view"] == "deliveries"

    cube_ratio = cube_pc_deliveries_with_5_stars[2]
    assert cube_ratio["name"] == "pc_deliveries_with_5_stars"
    assert cube_ratio["type"] == "number"
    assert cube_ratio["description"] == "Percentage of deliveries that received a 5-star rating."
    assert cube_ratio["title"] == "Deliveries with 5 stars (%)"
    assert cube_ratio["sql"] == "{pc_deliveries_with_5_stars_numerator} / nullif({pc_deliveries_with_5_stars_denominator}, 0)"
    assert cube_ratio["parent_view"] == "deliveries"


def test_ratio_metric_with_non_simple_numerator(monkeypatch, caplog):

    revenue = {
        "name": "revenue",
        "description": "Sum of the product revenue for each order item. Excludes tax.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "revenue_measure",
                "join_to_timespine": False,
                "fill_nulls_with": 0
            }
        },
        "label": "Revenue"
    }

    cumulative_revenue = {
        "name": "cumulative_revenue",
        "description": "The cumulative revenue for all orders.",
        "type": "cumulative",
        "type_params": {
            "measure": {
                "name": "revenue_measure",
                "join_to_timespine": False
            }
        },
        "label": "Cumulative Revenue (All Time)"
    }

    pc_revenue_of_total = {
        "name": "pc_revenue_of_total",
        "description": "The % of total revenue.",
        "type": "ratio",
        "type_params": {
            "numerator": {
                "name": "revenue",
            },
            "denominator": {
                "name": "cumulative_revenue",
            }
        },
        "label": "Revenue % of total"
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
                "name": "revenue_measure",
                "agg": "sum",
                "description": "The revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each product in an order.",
                "create_metric": False,
                "expr": "product_price"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_cube, "METRICS", [revenue, cumulative_revenue, pc_revenue_of_total])

    with caplog.at_level(logging.DEBUG):
        cube_measures = to_cube.metric_to_cube_measures(metric=pc_revenue_of_total, from_model=orders_model)

    assert any(record.levelname == 'DEBUG'
                and "non-simple denominator metrics are not supported." in record.message for record in caplog.records)
    
    assert len(cube_measures) == 0


def test_ratio_metric_with_numerator_and_denominator_from_different_models(monkeypatch, caplog):

    delivery_count = {
        "name": "delivery_count",
        "description": "Metric created from measure delivery_count",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "delivery_count_measure",
                "join_to_timespine": False
            }
        },
        "label": "delivery_count"
    }

    revenue = {
        "name": "revenue",
        "description": "Sum of the product revenue for each order item. Excludes tax.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "revenue_measure",
                "join_to_timespine": False,
                "fill_nulls_with": 0
            }
        },
        "label": "Revenue"
    }

    revenue_per_delivery = {
        "name": "revenue_per_delivery",
        "description": "The revenue per delivery.",
        "type": "ratio",
        "type_params": {
            "numerator": { "name": "revenue" },
            "denominator": { "name": "delivery_count" }
        },
        "label": "Revenue per Delivery"
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
                "name": "delivery_count_measure",
                "agg": "count",
                "create_metric": False,
                "expr": "delivery_id"
            }
        ]
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
                "name": "revenue_measure",
                "agg": "sum",
                "description": "The revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each product in an order.",
                "create_metric": False,
                "expr": "product_price"
            }
        ]
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model, orders_model])
    monkeypatch.setattr(to_cube, "METRICS", [delivery_count, revenue, revenue_per_delivery])

    with caplog.at_level(logging.DEBUG):
        cube_measures = to_cube.metric_to_cube_measures(metric=revenue_per_delivery, from_model=deliveries_model)

    assert any(record.levelname == 'DEBUG'
                and "numerator and denominator from different models not supported." in record.message for record in caplog.records)
    
    assert len(cube_measures) == 0