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
# TRANSLATE ORDERS
orders_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['orders'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/orders.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [orders_lkml_view]}))


# %%
# TRANSLATE DELIVERIES
deliveries_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['deliveries'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/deliveries.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [deliveries_lkml_view]}))

# %%
# TRANSLATE DELIVERY_PEOPLE
delivery_people_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['delivery_people'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/delivery_people.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [delivery_people_lkml_view]}))


# %%
# TRANSLATE CUSTOMERS
customers_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['customers'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/customers.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [customers_lkml_view]}))

# %%
# TRANSLATE LOCATIONS
locations_lkml_view = to_lkml.model_to_lkml_view(target_model=model_dict['locations'], 
                           metrics=semantic_manifest['metrics'],
                           models=semantic_manifest['semantic_models'])

with open('looker/locations.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [locations_lkml_view]}))