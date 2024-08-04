# %%
import json
import lkml
import mf_translate.to_lkml as to_lkml

import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')


# %%
with open('dbt/target/semantic_manifest.json') as f:
    semantic_manifest = json.load(f)

model_dict = {model['name']: model for model in semantic_manifest['semantic_models']}

# %%
orders_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['orders'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/orders.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [orders_lkml_view]}))


# %%
deliveries_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['deliveries'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/deliveries.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [orders_lkml_view]}))
# %%
