import mf_translate.to_cube as to_cube

def test_primary_key_entity(monkeypatch):

    customer_entity = {
        "name": "customer",
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

    monkeypatch.setattr(to_cube, 'DBT_NODES', nodes)

    customer_model = {
        "name": "customers",
        "node_relation": {
            "relation_name": "`mf_translate_db`.`jaffle_shop`.`customers`"
        }
    }

    cube_customer_dim = to_cube.entity_to_cube(entity=customer_entity,
                                               from_model=customer_model)

    assert cube_customer_dim["name"] == "customer"
    assert cube_customer_dim["description"] == "Customer identifier. Primary key."
    assert cube_customer_dim["type"] == "string"
    assert cube_customer_dim["public"] == False
    assert cube_customer_dim["sql"] == "{CUBE}.customer_id"


def test_foreign_key_entity(monkeypatch):

    order_entity = {
        "name": "order_id",
        "type": "foreign",
    }

    nodes = {
        "model.jaffle_shop.orders": {
            "columns": {
                "order_id": {}
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

    cube_order_dim = to_cube.entity_to_cube(entity=order_entity,
                                            from_model=orders_model)

    assert cube_order_dim["name"] == "order_id"
    assert "primary_key" not in cube_order_dim
    assert cube_order_dim["public"] == False
    assert cube_order_dim["sql"] == "{CUBE}.order_id"
    assert cube_order_dim["type"] == "string"