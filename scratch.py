# %%
import json
import lkml
import mf_translate.to_lkml as to_lkml

# %%
with open('dbt/target/semantic_manifest.json') as f:
    semantic_manifest = json.load(f)

lkml_view = {"views": [{"name": "test_view", "sql_table_name": "test_sql_table_name", "dimensions": []}]}

for model in semantic_manifest['semantic_models']:

    if model['name'] != 'orders':
        continue

    dimensions = model['dimensions']

    for dim in dimensions:
        lkml_dim = to_lkml.dimension_to_lkml(dim)

        lkml_view['views'][0]['dimensions'].append(lkml_dim)


print(lkml.dump(lkml_view))
# %%
