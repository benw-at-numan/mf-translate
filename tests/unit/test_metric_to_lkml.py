import mf_translate.to_lkml as to_lkml

def test_mf_dimension_ref_to_lookml():

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

    mf_dim_ref = "{{ Dimension('delivery__delivery_rating') }}"
    lkml_dim_ref = to_lkml.dimension_ref_to_lkml(dimension_reference=mf_dim_ref,
                                                 semantic_models=[deliveries_model])

    assert lkml_dim_ref == "${deliveries.delivery_rating}"

def test_mf_dimension_ref_to_lookml_2():

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

    mf_dim_ref = "{{    Dimension(  'order_id__discount_code'  )}}"
    lkml_dim_ref = to_lkml.dimension_ref_to_lkml(dimension_reference=mf_dim_ref,
                                                 semantic_models=[orders_model])

    assert lkml_dim_ref == "${orders.discount_code}"

