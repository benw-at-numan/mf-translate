# %%
import json
import lkml
import mf_translate.to_lkml as to_lkml

# %%
with open('dbt/target/semantic_manifest.json') as f:
    semantic_manifest = json.load(f)

lkml_view = {
    "views": [
        {
            "name": "test_view",
            "sql_table_name": "test_sql_table_name",
            "dimension_groups": [],
            "dimensions": [],
            "measures": []
        }
    ]
}

for model in semantic_manifest['semantic_models']:

    if model['name'] != 'orders':
        continue

    dimensions = model['dimensions']

    for dim in dimensions:
        lkml_dim = to_lkml.dimension_to_lkml(dim)

        if lkml_dim['type'] == 'time':
            lkml_view['views'][0]['dimension_groups'].append(lkml_dim)
        else:
            lkml_view['views'][0]['dimensions'].append(lkml_dim)

for metric in semantic_manifest['metrics']:

    print(metric)
    lkml_measures = to_lkml.metric_to_lkml_measures(metric, semantic_manifest['semantic_models'])

    print(lkml_measures)

    if lkml_measures:
        lkml_view['views'][0]['measures'].extend(lkml_measures)


print(lkml.dump(lkml_view))
# %%
