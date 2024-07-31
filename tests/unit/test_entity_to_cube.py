import mf_translate.to_cube as to_cube

def test_primary_key_entity():

    mf_customer_entity = {
        "name": "customer",
        "description": "Customer identifier. Primary key.",
        "type": "primary",
        "expr": "customer_id"
    }

    cube_customer_dim = to_cube.entity_to_cube(mf_customer_entity)

    assert cube_customer_dim["name"] == "customer"
    assert cube_customer_dim["description"] == "Customer identifier. Primary key."
    assert "type" not in cube_customer_dim
    assert cube_customer_dim["public"] == False
    assert cube_customer_dim["sql"] == "customer_id"


def test_foreign_key_entity():

    mf_order_entity = {
        "name": "order_id",
        "type": "foreign",
    }

    cube_order_dim = to_cube.entity_to_cube(mf_order_entity)

    assert cube_order_dim["name"] == "order_id"
    assert "primary_key" not in cube_order_dim
    assert cube_order_dim["public"] == False
    assert cube_order_dim["sql"] == "order_id"