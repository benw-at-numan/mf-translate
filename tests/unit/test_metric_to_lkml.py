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


    lkml_delivery_count = to_lkml.metric_to_lkml(metric=delivery_count,
                                                 models=[deliveries_model])

    assert lkml_delivery_count["name"] == "delivery_count"
    assert lkml_delivery_count["type"] == "count"
    "description" not in lkml_delivery_count
    "label" not in lkml_delivery_count
    "sql" not in lkml_delivery_count


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

    lkml_large_order_count = to_lkml.metric_to_lkml(metric=large_order_count,
                                                    models=[orders_model])

    assert lkml_large_order_count["name"] == "large_orders"
    assert lkml_large_order_count["type"] == "sum"
    assert lkml_large_order_count["description"] == "Count of orders with order total over 20."
    assert lkml_large_order_count["label"] == "Large Orders"
    assert lkml_large_order_count["sql"] == """
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

    lkml_large_order_count = to_lkml.metric_to_lkml(metric=large_order_count,
                                                    models=[orders_model])

    assert lkml_large_order_count["name"] == "large_orders"
    assert lkml_large_order_count["type"] == "sum"
    assert lkml_large_order_count["description"] == "Count of orders with order total over 20. Excludes staff orders."
    assert lkml_large_order_count["label"] == "Large Orders"
    assert lkml_large_order_count["sql"] == """
    case when (${orders.is_large_order} = true)
          and (${orders.is_staff_order} = false)
        then (1)
    end"""