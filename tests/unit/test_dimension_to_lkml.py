import mf_translate.to_lkml as to_lkml

# lkml parser will not accept keys with None values
def test_only_non_null_keys_translated():

    mf_dimension = {
        "name": "delivery_id",
        "description": None,
        "label": None,
        "type": None,
        "expr": None
    }

    lkml_dimension = to_lkml.dimension_to_lkml(mf_dimension)

    assert lkml_dimension["name"] == "delivery_id"
    assert 'description' not in lkml_dimension
    assert 'label' not in lkml_dimension
    assert 'type' not in lkml_dimension
    assert 'sql' not in lkml_dimension



def test_category_dimension():

    mf_delivery_rating = {
        "name": "delivery_rating",
        "description": "The rating the customer gave the delivery person.",
        "label": "Delivery Rating",
        "type": "categorical",
        "is_partition": False,
        "type_params": None,
        "expr": None
    }

    lkml_delivery_rating = to_lkml.dimension_to_lkml(mf_delivery_rating)

    assert lkml_delivery_rating["name"] == "delivery_rating"
    assert lkml_delivery_rating["description"] == "The rating the customer gave the delivery person."
    assert lkml_delivery_rating["label"] == "Delivery Rating"
    assert "type" not in lkml_delivery_rating


def test_category_dim_with_expr():

    mf_is_bulk_transaction = {
        "name": "is_bulk_transaction",
        "type": "categorical",
        "expr": "case when quantity > 10 then true else false end",
    }

    lkml_is_bulk_transaction = to_lkml.dimension_to_lkml(mf_is_bulk_transaction)

    assert lkml_is_bulk_transaction["name"] == "is_bulk_transaction"
    assert "type" not in lkml_is_bulk_transaction
    assert lkml_is_bulk_transaction["sql"] == "case when quantity > 10 then true else false end"


def test_time_dimension():

    mf_create_date = {
        "name": "created_at",
        "type": "time",
        "label": "Date of creation",
        "expr": "date_trunc('day', ts_created)",
        "type_params": {
            "time_granularity": "day"
        }
    }

    lkml_create_date = to_lkml.dimension_to_lkml(mf_create_date)

    assert lkml_create_date["name"] == "created_at"
    assert lkml_create_date["type"] == "time"
    assert lkml_create_date["timeframes"] == ['date', 'week', 'month', 'quarter', 'year']
    assert lkml_create_date["label"] == "Date of creation"
    assert lkml_create_date["sql"] == "date_trunc('day', ts_created)"


def test_monthly_time_dimension():

    mf_invoice_month = {
        "name": "invoice_month",
        "type": "time",
        "label": "Month of invoice",
        "expr": "date_trunc('month', ts_invoiced)",
        "type_params": {
            "time_granularity": "month"
        }
    }

    lkml_invoice_month = to_lkml.dimension_to_lkml(mf_invoice_month)

    assert lkml_invoice_month["name"] == "invoice_month"
    assert lkml_invoice_month["type"] == "time"
    assert lkml_invoice_month["timeframes"] == ['month', 'quarter', 'year']
    assert lkml_invoice_month["label"] == "Month of invoice"
    assert lkml_invoice_month["sql"] == "date_trunc('month', ts_invoiced)"


def test_time_dim_without_granularity():

    mf_order_date = {
        "name": "ordered_at_test",
        "type": "time",
        "label": "Time of order",
        "expr": "ts_ordered"
    }

    lkml_order_date = to_lkml.dimension_to_lkml(mf_order_date)

    assert lkml_order_date["name"] == "ordered_at_test"
    assert lkml_order_date["type"] == "date_time"
    assert "timeframes" not in lkml_order_date
    assert lkml_order_date["label"] == "Time of order"
    assert lkml_order_date["sql"] == "ts_ordered"