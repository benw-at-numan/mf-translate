import mf_translate.to_cube as to_cube

def test_basic_model_to_cube_view(monkeypatch):

    deliveries_model = {
        "name": "deliveries",
        "defaults": {
            "agg_time_dimension": "delivered_at"
        },
        "description": "Delivery fact table. This table is at the delivery grain with one row per delivery.\n",
        "node_relation": {
            "alias": "deliveries",
            "schema_name": "jaffle_shop",
            "database": "fresh-iridium-428713-j5",
            "relation_name": "`fresh-iridium-428713-j5`.`jaffle_shop`.`deliveries`"
        },
        "entities": [
            {
                "name": "delivery_id",
                "type": "primary"
            },
            {
                "name": "order_id",
                "type": "foreign"
            },
            {
                "name": "delivery_person_id",
                "type": "foreign",
            }
        ],
        "dimensions": [
            {
                "name": "delivered_at",
                "type": "time",
                "is_partition": False,
                "type_params": {
                    "time_granularity": "day",
                },
            },
            {
                "name": "delivery_rating",
                "type": "categorical",
                "is_partition": False,
            }
        ],
        "measures": [
            {
                "name": "delivery_count",
                "agg": "count",
                "create_metric": False,
                "expr": "delivery_id",
            }
        ]
    }

    delivery_count = {
        "name": "delivery_count",
        "description": "Metric created from measure delivery_count",
        "type": "simple",
        "type_params": {
            "measure": {
                "name": "delivery_count",
                "join_to_timespine": False
            },
            "expr": "delivery_count"
        },
        "label": "delivery_count"
    }

    monkeypatch.setattr(to_cube, "SEMANTIC_MODELS", [deliveries_model])
    monkeypatch.setattr(to_cube, "METRICS", [delivery_count])

    cube_view = to_cube.model_to_cube_cube(deliveries_model)

    assert cube_view["name"] == "deliveries"
    assert cube_view['sql_table'] == "`fresh-iridium-428713-j5`.`jaffle_shop`.`deliveries`"
    assert any(dim['name'] == 'delivery_id' for dim in cube_view['dimensions'])
    assert any(dim['name'] == 'order_id' for dim in cube_view['dimensions'])
    assert any(dim['name'] == 'delivery_person_id' for dim in cube_view['dimensions'])
    assert any(dim['name'] == 'delivery_rating' for dim in cube_view['dimensions'])
    assert any(dim['name'] == 'delivered_at' for dim in cube_view['dimensions'])
    assert any(measure['name'] == 'delivery_count' for measure in cube_view['measures'])
