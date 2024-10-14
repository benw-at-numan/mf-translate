import mf_compare_query.to_looker as to_looker

def test_single_metric(monkeypatch):

    semantic_models = [ 
        { 
            "name": "orders",
            "entities": [],
            "measures": [
                {
                    "name": "order_total"
                }
            ]
        }
    ]
    monkeypatch.setattr(to_looker, 'SEMANTIC_MODELS', semantic_models)

    metrics = [
        {
            "name": "order_total",
            "type_params": {
                "input_measures": [
                    {
                        "name": "order_total",
                    }
                ]
            }
        }
    ]
    monkeypatch.setattr(to_looker, 'METRICS', metrics)

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_PROJECT", "looker_model")
    lkr_query = to_looker.query_to_looker_query(metrics=['order_total'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total']
    assert lkr_query.sorts == None


def test_single_metric_with_group_and_order_by(monkeypatch):

    semantic_models = [
        {
            "name": "orders",
            "entities": [],
            "measures": [
                {
                    "name": "order_total"
                }
            ]
        },
        {  
            "name": "locations",
            "entities": [
                {
                    "name": "location_id",
                    "type": "primary"
                }
            ],
            "measures": []
        }
    ]
    monkeypatch.setattr(to_looker, 'SEMANTIC_MODELS', semantic_models)

    metrics = [
        {
            "name": "order_total",
            "type_params": {
                "input_measures": [
                    {
                        "name": "order_total",
                    }
                ]
            }
        }
    ]
    monkeypatch.setattr(to_looker, 'METRICS', metrics)

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_PROJECT", "looker_model")
    lkr_query = to_looker.query_to_looker_query(metrics=['order_total'], group_by=['location_id__location_name'], order_by=['-location_id__location_name'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'locations.location_name']
    assert lkr_query.sorts == ['-locations.location_name']


def test_single_metric_with_group_and_order_by_metric(monkeypatch):

    semantic_models = [
        {
            "name": "orders",
            "entities": [],
            "measures": [
                {
                    "name": "order_total"
                }
            ]
        },
        {  
            "name": "locations",
            "entities": [
                {
                    "name": "location_id",
                    "type": "primary"
                }
            ],
            "measures": []
        }
    ]
    monkeypatch.setattr(to_looker, 'SEMANTIC_MODELS', semantic_models)

    metrics = [
        {
            "name": "order_total",
            "type_params": {
                "input_measures": [
                    {
                        "name": "order_total",
                    }
                ]
            }
        }
    ]
    monkeypatch.setattr(to_looker, 'METRICS', metrics)

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_PROJECT", "looker_model")
    lkr_query = to_looker.query_to_looker_query(metrics=['order_total'], group_by=['location_id__location_name'], order_by=['-order_total'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'locations.location_name']
    assert lkr_query.sorts == ['-orders.order_total']