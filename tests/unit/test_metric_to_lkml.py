import mf_translate.to_lkml as to_lkml
import logging
import re


def test_filter_expression():

    deliveries_model= {
        "name": "deliveries",
        "entities": [
            {
                "name": "delivery",
                "type": "primary",
                "expr": "delivery_id"
            }
        ]
    }

    mf_filter = "{{Dimension('delivery__delivery_rating')}} = 5"
    lkml_filter = to_lkml.sql_expression_to_lkml(expression=mf_filter,
                                                 models=[deliveries_model])

    assert lkml_filter == "${deliveries.delivery_rating} = 5"


def test_another_filter_expression():

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

    mf_filter = "coalesce( {{ Dimension( 'order_id__discount_code'  ) }}, 'NO_DISCOUNT' ) != 'STAFF_ORDER'"
    lkml_filter = to_lkml.sql_expression_to_lkml(expression=mf_filter,
                                                 models=[orders_model])

    assert lkml_filter == "coalesce( ${orders.discount_code}, 'NO_DISCOUNT' ) != 'STAFF_ORDER'"


def test_calculation_expression():

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

    mf_calc = "{{ Dimension('order_id__revenue') }} - {{ Dimension('order_id__discount') }}"
    lkml_calc = to_lkml.sql_expression_to_lkml(expression=mf_calc,
                                               models=[orders_model])

    assert lkml_calc == "${orders.revenue} - ${orders.discount}"


def test_simple_metric():

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
                "create_metric": False,
                "expr": "delivery_id"
            }
        ]
    }


    lkml_delivery_count = to_lkml.metric_to_lkml_measures(target_metric=delivery_count,
                                                          models=[deliveries_model])

    lkml_measure = lkml_delivery_count[0]

    assert lkml_measure["name"] == "delivery_count"
    assert lkml_measure["type"] == "count"
    assert lkml_measure["description"] == "Metric created from measure delivery_count"
    assert lkml_measure["label"] == "delivery_count"
    assert "sql" not in lkml_measure
    assert lkml_measure["parent_view"] == "deliveries"


def test_another_simple_metric():

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
                "create_metric": False,
                "expr": None
            }
        ]
    }

    lkml_order_total = to_lkml.metric_to_lkml_measures(target_metric=order_total,
                                                       models=[orders_model])

    lkml_measure = lkml_order_total[0]
    assert lkml_measure["name"] == "order_total"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Sum of total order amonunt. Includes tax + revenue."
    assert lkml_measure["label"] == "Order Total"
    assert lkml_measure["sql"] == "order_total"
    assert lkml_measure["parent_view"] == "orders"


def normalised_strings_equal(string1, string2):
    # Replace multiple whitespace characters (including new lines) with a single space
    normalised_string1 = re.sub(r'\s+', ' ', string1).strip()
    normalised_string2 = re.sub(r'\s+', ' ', string2).strip()

    return normalised_string1 == normalised_string2


def test_metric_with_category_filter():

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

    lkml_large_order_count = to_lkml.metric_to_lkml_measures(target_metric=orders_for_returning_customers,
                                                             models=[customers_model, orders_model])

    lkml_measure = lkml_large_order_count[0]

    assert lkml_measure["name"] == "orders_for_returning_customers"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Count of orders from returning customers."
    assert lkml_measure["label"] == "Returning customer orders"
    assert normalised_strings_equal(lkml_measure["sql"],
                                    """
                                    case when (${customers.customer_type} = 'returning')
                                        then (1)
                                    end
                                    """)
    assert lkml_measure["parent_view"] == "orders"


def test_metric_with_multiple_category_filters():

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

    lkml_large_order_count = to_lkml.metric_to_lkml_measures(target_metric=large_order_count,
                                                             models=[orders_model])

    lkml_measure = lkml_large_order_count[0]
    assert lkml_measure["name"] == "large_orders"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Count of orders with order total over 20. Excludes staff orders."
    assert lkml_measure["label"] == "Large Orders"
    assert normalised_strings_equal(lkml_measure["sql"],
                                    """
                                    case when (${orders.is_large_order} = true)
                                           and (${orders.is_staff_order} = false)
                                        then (1)
                                    end
                                    """)
    assert lkml_measure["parent_view"] == "orders"


def test_ratio_metric():

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
        ]
    }

    lkml_food_revenue_pct = to_lkml.metric_to_lkml_measures(target_metric=food_revenue_pct,
                                                            models=[orders_model],
                                                            metrics=[food_revenue_pct, food_revenue, revenue])

    lkml_numerator = lkml_food_revenue_pct[0]
    assert lkml_numerator["name"] == "food_revenue_pct_numerator"
    assert lkml_numerator["hidden"] == 'Yes'
    assert lkml_numerator["type"] == "sum"
    assert 'description' not in lkml_numerator
    assert 'label' not in lkml_numerator
    assert lkml_numerator["sql"] == "case when is_food_item = 1 then product_price else 0 end"
    assert lkml_numerator["parent_view"] == "orders"

    lkml_denominator = lkml_food_revenue_pct[1]
    assert lkml_denominator["name"] == "food_revenue_pct_denominator"
    assert lkml_denominator["hidden"] == 'Yes'
    assert lkml_denominator["type"] == "sum"
    assert 'description' not in lkml_denominator
    assert 'label' not in lkml_denominator
    assert lkml_denominator["sql"] == "product_price"
    assert lkml_denominator["parent_view"] == "orders"

    lkml_ratio = lkml_food_revenue_pct[2]
    assert lkml_ratio["name"] == "food_revenue_pct"
    assert lkml_ratio["type"] == "number"
    assert lkml_ratio["description"] == "The % of order revenue from food."
    assert lkml_ratio["label"] == "Food Revenue %"
    assert lkml_ratio["sql"] == "${food_revenue_pct_numerator} / nullif(${food_revenue_pct_denominator}, 0)"
    assert lkml_ratio["parent_view"] == "orders"


def test_filtered_ratio_metric():

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
        "measures": []
    }

    lkml_pc_deliveries_with_5_stars = to_lkml.metric_to_lkml_measures(target_metric=pc_deliveries_with_5_stars,
                                                                      models=[deliveries_model, orders_model],
                                                                      metrics=[pc_deliveries_with_5_stars, delivery_count])

    lkml_numerator = lkml_pc_deliveries_with_5_stars[0]
    assert lkml_numerator["name"] == "pc_deliveries_with_5_stars_numerator"
    assert lkml_numerator["hidden"] == 'Yes'
    assert lkml_numerator["type"] == "count_distinct"
    assert 'description' not in lkml_numerator
    assert 'label' not in lkml_numerator
    assert normalised_strings_equal(lkml_numerator["sql"],
                                    """
                                    case when (${deliveries.delivery_rating} = 5)
                                          and (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
                                        then (delivery_id)
                                    end
                                    """)
    assert lkml_numerator["parent_view"] == "deliveries"

    lkml_denominator = lkml_pc_deliveries_with_5_stars[1]
    assert lkml_denominator["name"] == "pc_deliveries_with_5_stars_denominator"
    assert lkml_denominator["hidden"] == 'Yes'
    assert lkml_denominator["type"] == "count_distinct"
    assert 'description' not in lkml_denominator
    assert 'label' not in lkml_denominator
    assert normalised_strings_equal(lkml_denominator["sql"],
                                    """
                                    case when (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
                                        then (delivery_id)
                                    end
                                    """)
    assert lkml_denominator["parent_view"] == "deliveries"

    lkml_ratio = lkml_pc_deliveries_with_5_stars[2]
    assert lkml_ratio["name"] == "pc_deliveries_with_5_stars"
    assert lkml_ratio["type"] == "number"
    assert lkml_ratio["description"] == "Percentage of deliveries that received a 5-star rating."
    assert lkml_ratio["label"] == "Deliveries with 5 stars (%)"
    assert lkml_ratio["sql"] == "${pc_deliveries_with_5_stars_numerator} / nullif(${pc_deliveries_with_5_stars_denominator}, 0)"
    assert lkml_ratio["parent_view"] == "deliveries"


def test_ratio_metric_with_non_simple_numerator(caplog):

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

    with caplog.at_level(logging.WARNING):
        to_lkml.metric_to_lkml_measures(target_metric=pc_revenue_of_total,
                                        models=[orders_model],
                                        metrics=[revenue, cumulative_revenue, pc_revenue_of_total])

    assert any(record.levelname == 'WARNING'
                and "non-simple denominator metrics are not currently supported." in record.message for record in caplog.records)


def test_ratio_metric_with_numerator_and_denominator_from_different_models(caplog):

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

    with caplog.at_level(logging.WARNING):
        to_lkml.metric_to_lkml_measures(target_metric=revenue_per_delivery,
                                        models=[deliveries_model, orders_model],
                                        metrics=[delivery_count, revenue, revenue_per_delivery])

    assert any(record.levelname == 'WARNING'
                and "numerator and denominator from different models not currently supported." in record.message for record in caplog.records)