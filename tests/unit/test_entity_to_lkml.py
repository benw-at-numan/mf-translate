import mf_translate.to_lkml as to_lkml

def test_primary_key_entity():

    customer_entity = {
        "name": "customer",
        "description": "Customer identifier. Primary key.",
        "type": "primary",
        "expr": "customer_id"
    }

    lkml_customer_dim = to_lkml.entity_to_lkml(customer_entity)

    assert lkml_customer_dim["name"] == "customer"
    assert lkml_customer_dim["description"] == "Customer identifier. Primary key."
    assert "type" not in lkml_customer_dim
    assert lkml_customer_dim["hidden"] == 'Yes'
    assert lkml_customer_dim["sql"] == "customer_id"
    assert lkml_customer_dim["primary_key"] == 'Yes'


def test_foreign_key_entity():

    order_entity = {
        "name": "order_id",
        "type": "foreign",
    }

    lkml_order_dim = to_lkml.entity_to_lkml(order_entity)

    assert lkml_order_dim["name"] == "order_id"
    assert "primary_key" not in lkml_order_dim
    assert lkml_order_dim["hidden"] == 'Yes'
    assert "sql" not in lkml_order_dim

def test_foreign_key_entity_with_custom_sql():

    order_entity = {
        "name": "delivery_person_id",
        "type": "foreign",
        "expr": "delivery_man_id"
    }

    lkml_order_dim = to_lkml.entity_to_lkml(order_entity)

    assert lkml_order_dim["name"] == "delivery_person_id"
    assert "primary_key" not in lkml_order_dim
    assert lkml_order_dim["hidden"] == 'Yes'
    assert lkml_order_dim["sql"] == "delivery_man_id"