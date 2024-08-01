import mf_translate.to_lkml as to_lkml


def test_filter_to_lookml():

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
    lkml_filter = to_lkml.filter_to_lkml(mf_filter=mf_filter,
                                         mf_models=[deliveries_model])

    assert lkml_filter == "${deliveries.delivery_rating} = 5"


def test_another_filter_to_lookml():

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
    lkml_filter = to_lkml.filter_to_lkml(mf_filter=mf_filter,
                                         mf_models=[orders_model])

    assert lkml_filter == "coalesce( ${orders.discount_code}, 'NO_DISCOUNT' ) != 'STAFF_ORDER'"