import mf_translate.to_looker as to_looker

def test_primary_key_entity(monkeypatch):

    customer_entity = {
        "name": "customer",
        "label": "Customer",
        "description": "Customer identifier. Primary key.",
        "type": "primary",
        "expr": "customer_id"
    }

    nodes = {
        "model.jaffle_shop.customers": {
            "columns": {
                "customer_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`customers`"
        }
    }

    customer_model = {
        "name": "customers",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`customers`"
        }
    }

    monkeypatch.setattr(to_looker, 'DBT_NODES', nodes)

    lkml_customer_dim = to_looker.entity_to_lkml(entity=customer_entity,
                                                 from_model=customer_model)

    assert lkml_customer_dim["name"] == "customer"
    assert lkml_customer_dim["label"] == "Customer"
    assert lkml_customer_dim["description"] == "Customer identifier. Primary key."
    assert "type" not in lkml_customer_dim
    assert lkml_customer_dim["hidden"] == 'yes'
    assert lkml_customer_dim["sql"] == "${TABLE}.customer_id"
    assert lkml_customer_dim["primary_key"] == 'yes'


def test_foreign_key_entity():

    order_entity = {
        "name": "order_id",
        "type": "foreign",
    }

    orders_model = {
        "name": "orders"
    }

    lkml_order_dim = to_looker.entity_to_lkml(entity=order_entity,
                                              from_model=orders_model)

    assert lkml_order_dim["name"] == "order_id"
    assert "primary_key" not in lkml_order_dim
    assert lkml_order_dim["hidden"] == 'yes'
    assert "sql" not in lkml_order_dim

def test_foreign_key_entity_with_custom_sql(monkeypatch):

    delivery_person_entity = {
        "name": "delivery_person_id",
        "type": "foreign",
        "expr": "delivery_man_id"
    }

    orders_model = {
        "name": "orders",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {},
                "delivery_man_id": {}
            },
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`orders`"
        }
    }

    monkeypatch.setattr(to_looker, 'DBT_NODES', nodes)

    lkml_order_dim = to_looker.entity_to_lkml(entity=delivery_person_entity,
                                              from_model=orders_model)

    assert lkml_order_dim["name"] == "delivery_person_id"
    assert "primary_key" not in lkml_order_dim
    assert lkml_order_dim["hidden"] == 'yes'
    assert lkml_order_dim["sql"] == "${TABLE}.delivery_man_id"