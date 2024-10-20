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

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total']
    assert lkr_query.sorts == None


def test_single_metric_with_group_by_entity(monkeypatch):

    semantic_models = [
        {
            "name": "orders",
            "entities": [
                {
                    "name": "order_id",
                    "type": "primary"
                },
                {
                    "name": "customer_id",
                    "type": "foreign"
                }
            ],
            "measures": [
                {
                    "name": "order_total"
                }
            ]
        },
        {
            "name": "customers",
            "entities": [
                {
                    "name": "customer_id",
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

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total'], group_by=['customer_id'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'customers.customer_id']
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
            "dimensions": [
                {
                    "name": "location_name",
                    "type": "categorical"
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

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total'], group_by=['location_id__location_name'], order_by=['-location_id__location_name'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'locations.location_name']
    assert lkr_query.sorts == ['-locations.location_name']


def test_single_metric_with_group_and_order_by_across_models_with_duplicated_entitiy_id(monkeypatch):

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
            "dimensions": [
                {
                    "name": "location_name",
                    "type": "categorical"
                }
            ],
            "measures": []
        },
        {
            "name": "alternative_locations",
            "entities": [
                {
                    "name": "location_id",
                    "type": "primary"
                }
            ],
            "dimensions": [
                {
                    "name": "alternative_location_name",
                    "type": "categorical"
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

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total'], group_by=['location_id__location_name'], order_by=['-location_id__location_name'])

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
            "dimensions": [
                {
                    "name": "location_name",
                    "type": "categorical"
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

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total'], group_by=['location_id__location_name'], order_by=['-order_total'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'locations.location_name']
    assert lkr_query.sorts == ['-orders.order_total']


def test_multiple_metrics(monkeypatch):

    semantic_models = [
        {
            "name": "orders",
            "entities": [],
            "measures": [
                {
                    "name": "order_total"
                },
                {
                    "name": "order_count"
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
        },
        {
            "name": "order_count",
            "type_params": {
                "input_measures": [
                    {
                        "name": "order_count",
                    }
                ]
            }
        }
    ]
    monkeypatch.setattr(to_looker, 'METRICS', metrics)

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total', 'order_count'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'orders.order_count']
    assert lkr_query.sorts == None


def test_metrics_across_different_models(monkeypatch):

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
            "entities": [],
            "measures": [
                {
                    "name": "location_count"
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
        },
        {
            "name": "location_count",
            "type_params": {
                "input_measures": [
                    {
                        "name": "location_count",
                    }
                ]
            }
        }
    ]
    monkeypatch.setattr(to_looker, 'METRICS', metrics)

    monkeypatch.setenv("MF_TRANSLATE_LOOKER_MODEL", "looker_model")
    lkr_query = to_looker.query_to_looker_query(explore='orders', metrics=['order_total', 'location_count'])

    assert lkr_query.model == 'looker_model'
    assert lkr_query.view == 'orders'
    assert lkr_query.fields == ['orders.order_total', 'locations.location_count']
    assert lkr_query.sorts == None