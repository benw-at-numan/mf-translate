# %%
import json
import lkml
import mf_translate.to_lkml as to_lkml

import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


# %%
with open('dbt/target/semantic_manifest.json') as f:
    semantic_manifest = json.load(f)

lkml_view = {
    "name": "orders",
    "sql_table_name": "test_sql_table_name",
    "dimension_groups": [],
    "dimensions": [],
    "measures": []
}

for model in semantic_manifest['semantic_models']:

    if model['name'] != lkml_view['name']:
        continue

    dimensions = model['dimensions']

    for entity in model['entities']:
        lkml_dim = to_lkml.entity_to_lkml(entity)
        lkml_view['dimensions'].append(lkml_dim)

    for dim in dimensions:
        lkml_dim = to_lkml.dimension_to_lkml(dim)

        if lkml_dim['type'] == 'time':
            lkml_view['dimension_groups'].append(lkml_dim)
        else:
            lkml_view['dimensions'].append(lkml_dim)

for metric in semantic_manifest['metrics']:

    lkml_measures = to_lkml.metric_to_lkml_measures(metric=metric,
                                                    models=semantic_manifest['semantic_models'],
                                                    metrics=semantic_manifest['metrics'])

    for lkml_measure in lkml_measures:
        if lkml_measure['parent_view'] == lkml_view['name']:
            del lkml_measure['parent_view']
            lkml_view['measures'].append(lkml_measure)


logging.info(lkml.dump({'views': [lkml_view]}))
# %%
