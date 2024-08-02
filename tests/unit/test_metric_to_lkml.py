import mf_translate.to_lkml as to_lkml


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
            "expr": "delivery_count",
            "metrics": [],
            "input_measures": [
                {
                    "name": "delivery_count",
                    "join_to_timespine": False,
                }
            ]
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


    lkml_delivery_count = to_lkml.metric_to_lkml_measures(metric=delivery_count,
                                                 models=[deliveries_model])

    lkml_measure = lkml_delivery_count[0]

    assert lkml_measure["name"] == "delivery_count"
    assert lkml_measure["type"] == "count"
    "description" not in lkml_measure
    "label" not in lkml_measure
    "sql" not in lkml_measure


def test_another_simple_metric():

    order_total = {
        "name": "order_total",
        "description": "Sum of total order amonunt. Includes tax + revenue.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_total",
                "join_to_timespine": False
            },
            "metrics": [],
            "input_measures": [
                {
                    "name": "order_total",
                    "join_to_timespine": False
                }
            ]
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

    lkml_order_total = to_lkml.metric_to_lkml_measures(metric=order_total,
                                                       models=[orders_model])

    lkml_measure = lkml_order_total[0]
    assert lkml_measure["name"] == "order_total"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Sum of total order amonunt. Includes tax + revenue."
    assert lkml_measure["label"] == "Order Total"
    assert lkml_measure["sql"] == "order_total"


def test_metric_with_category_filter():

    large_order_count = {
        "name": "large_orders",
        "description": "Count of orders with order total over 20.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_count",
                "join_to_timespine": False
            },
            "metrics": [],
            "input_measures": [
                {
                    "name": "order_count",
                    "join_to_timespine": False
                }
            ]
        },
        "filter": {
            "where_filters": [
                {
                    "where_sql_template": "{{ Dimension('order_id__is_large_order') }} = true\n"
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

    lkml_large_order_count = to_lkml.metric_to_lkml_measures(metric=large_order_count,
                                                    models=[orders_model])

    lkml_measure = lkml_large_order_count[0]

    assert lkml_measure["name"] == "large_orders"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Count of orders with order total over 20."
    assert lkml_measure["label"] == "Large Orders"
    assert lkml_measure["sql"] == """
    case when (${orders.is_large_order} = true)
        then (1)
    end"""

def test_metric_with_multiple_category_filters():

    large_order_count = {
        "name": "large_orders",
        "description": "Count of orders with order total over 20. Excludes staff orders.",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "order_count",
                "join_to_timespine": False
            },
            "metrics": [],
            "input_measures": [
                {
                    "name": "order_count",
                    "join_to_timespine": False
                }
            ]
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

    lkml_large_order_count = to_lkml.metric_to_lkml_measures(metric=large_order_count,
                                                             models=[orders_model])

    lkml_measure = lkml_large_order_count[0]
    assert lkml_measure["name"] == "large_orders"
    assert lkml_measure["type"] == "sum"
    assert lkml_measure["description"] == "Count of orders with order total over 20. Excludes staff orders."
    assert lkml_measure["label"] == "Large Orders"
    assert lkml_measure["sql"] == """
    case when (${orders.is_large_order} = true)
          and (${orders.is_staff_order} = false)
        then (1)
    end"""


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
            },
            "metrics": [],
            "input_measures": [
                {
                    "name": "food_revenue",
                    "join_to_timespine": False
                },
                {
                    "name": "revenue",
                    "join_to_timespine": False,
                    "fill_nulls_with": 0
                }
            ]
        },
        "label": "Food Revenue %"
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
                "name": "revenue",
                "agg": "sum",
                "description": "The revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each product in an order.",
                "create_metric": False,
                "expr": "product_price"
            },
            {
                "name": "food_revenue",
                "agg": "sum",
                "description": "The food revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each food product in an order.",
                "create_metric": False,
                "expr": "case when is_food_item = 1 then product_price else 0 end"
            }
        ]
    }

    lkml_food_revenue_pct = to_lkml.metric_to_lkml_measures(metric=food_revenue_pct,
                                                            models=[orders_model])

    lkml_numerator = lkml_food_revenue_pct[0]
    assert lkml_numerator["name"] == "food_revenue_pct_numerator"
    assert lkml_numerator["hidden"] == True
    assert lkml_numerator["type"] == "sum"
    assert lkml_numerator["description"] == "The food revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each food product in an order."
    assert lkml_numerator["sql"] == "case when is_food_item = 1 then product_price else 0 end"

    lkml_denominator = lkml_food_revenue_pct[1]
    assert lkml_denominator["name"] == "food_revenue_pct_denominator"
    assert lkml_denominator["hidden"] == True
    assert lkml_denominator["type"] == "sum"
    assert lkml_denominator["description"] == "The revenue generated for each order item. Revenue is calculated as a sum of revenue associated with each product in an order."
    assert lkml_denominator["sql"] == "product_price"

    lkml_ratio = lkml_food_revenue_pct[2]
    assert lkml_ratio["name"] == "food_revenue_pct"
    assert lkml_ratio["type"] == "number"
    assert lkml_ratio["description"] == "The % of order revenue from food."
    assert lkml_ratio["label"] == "Food Revenue %"
    assert lkml_ratio["sql"] == "${food_revenue_pct_numerator} / nullif(${food_revenue_pct_denominator}, 0)"


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
            },
            "metrics": [],
            "input_measures": [
                {
                    "name": "delivery_count",
                    "join_to_timespine": False
                }
            ]
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

    lkml_pc_deliveries_with_5_stars = to_lkml.metric_to_lkml_measures(metric=pc_deliveries_with_5_stars,
                                                                      models=[deliveries_model, orders_model])

    lkml_numerator = lkml_pc_deliveries_with_5_stars[0]
    assert lkml_numerator["name"] == "pc_deliveries_with_5_stars_numerator"
    assert lkml_numerator["hidden"] == True
    assert lkml_numerator["type"] == "count_distinct"
    assert 'description' not in lkml_numerator
    assert lkml_numerator["sql"] == """
    case when (${deliveries.delivery_rating} = 5)
          and (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
        then (delivery_id)
    end"""

    lkml_denominator = lkml_pc_deliveries_with_5_stars[1]
    assert lkml_denominator["name"] == "pc_deliveries_with_5_stars_denominator"
    assert lkml_denominator["hidden"] == True
    assert lkml_denominator["type"] == "count_distinct"
    assert 'description' not in lkml_denominator
    assert lkml_denominator["sql"] == """
    case when (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
        then (delivery_id)
    end"""

    lkml_ratio = lkml_pc_deliveries_with_5_stars[2]
    assert lkml_ratio["name"] == "pc_deliveries_with_5_stars"
    assert lkml_ratio["type"] == "number"
    assert lkml_ratio["description"] == "Percentage of deliveries that received a 5-star rating."
    assert lkml_ratio["label"] == "Deliveries with 5 stars (%)"
    assert lkml_ratio["sql"] == "${pc_deliveries_with_5_stars_numerator} / nullif(${pc_deliveries_with_5_stars_denominator}, 0)"