import mf_translate.to_ldsh as to_ldsh

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

    monkeypatch.setattr(to_ldsh, "SEMANTIC_MODELS", [orders_model])

    mf_calc = "{{ Dimension('order_id__revenue') }} - {{ Dimension('order_id__discount') }}"
    ldsh_calc = to_ldsh.sql_expression_to_ldsh(expression=mf_calc,
                                               from_model=orders_model)

    assert ldsh_calc == "${revenue} - ${discount}"


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

    monkeypatch.setattr(to_ldsh, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_ldsh, 'DBT_NODES', nodes)

    mf_calc = "{{ Dimension('order_id__revenue') }} - discount"
    ldsh_calc = to_ldsh.sql_expression_to_ldsh(expression=mf_calc,
                                               from_model=orders_model)

    assert ldsh_calc == "${revenue} - ${TABLE}.discount"

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
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        },
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
        ]
    }

    nodes = {
        "model.jaffle_shop.deliveries": {
            "columns": {
                "delivery_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`deliveries`"
        }
    }

    monkeypatch.setattr(to_ldsh, "SEMANTIC_MODELS", [deliveries_model])
    monkeypatch.setattr(to_ldsh, 'DBT_NODES', nodes)

    ldsh_delivery_count = to_ldsh.metric_to_ldsh_measures(metric=delivery_count,
                                                          from_model=deliveries_model)

    ldsh_measure = ldsh_delivery_count[0]

    assert ldsh_measure["name"] == "delivery_count"
    assert ldsh_measure["type"] == "count"
    assert "description" not in ldsh_measure
    assert "label" not in ldsh_measure
    assert ldsh_measure["sql"] == "${TABLE}.delivery_id"
    assert ldsh_measure["parent_view"] == "deliveries"

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

    monkeypatch.setattr(to_ldsh, "SEMANTIC_MODELS", [orders_model])
    monkeypatch.setattr(to_ldsh, 'DBT_NODES', nodes)

    ldsh_order_total = to_ldsh.metric_to_ldsh_measures(metric=order_total,
                                                       from_model=orders_model)

    ldsh_measure = ldsh_order_total[0]
    assert ldsh_measure["name"] == "order_total"
    assert ldsh_measure["type"] == "sum"
    assert ldsh_measure["description"] == "Sum of total order amonunt. Includes tax + revenue."
    assert ldsh_measure["label"] == "Order Total"
    assert ldsh_measure["sql"] == "${TABLE}.order_total"
    assert ldsh_measure["parent_view"] == "orders"