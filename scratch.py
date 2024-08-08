# %%
# IMPORT REQUIREMENTS
import json
import lkml
import mf_translate.to_lkml as to_lkml
import os
import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

from dotenv import load_dotenv
load_dotenv()

# %%
# LOAD MANIFESTS
manifest_dir = os.getenv('MF_TRANSLATE__DBT_MANIFIEST_DIR')
if not manifest_dir:
    raise ValueError("Manifest directory must be provided.")
    
manifest_dir = manifest_dir.rstrip('/')

semantic_manifest = {}
try:
    with open(f'{manifest_dir}/semantic_manifest.json') as f:
        semantic_manifest = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"The file {f'{manifest_dir}/semantic_manifest.json'} does not exist.")
except json.JSONDecodeError:
    raise ValueError(f"The file {f'{manifest_dir}/semantic_manifest.json'} is not a valid JSON file.")

model_dict = {model['name']: model for model in semantic_manifest['semantic_models']}

manifest = {}
try:
    with open(f'{manifest_dir}/manifest.json') as f:
        manifest = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"The file {f'{manifest_dir}/manifest.json'} does not exist.")
except json.JSONDecodeError:
    raise ValueError(f"The file {f'{manifest_dir}/manifest.json'} is not a valid JSON file.")

to_lkml.set_manifests(metricflow_semantic_manifest=semantic_manifest,
                      dbt_manifest=manifest)

# %%
# TRANSLATE ORDERS
orders_lkml_view = to_lkml.model_to_lkml_view(model=model_dict['orders'])

with open('looker/orders.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [orders_lkml_view]}))


# %%
# TRANSLATE DELIVERIES
deliveries_lkml_view = to_lkml.model_to_lkml_view(model=model_dict['deliveries'])

with open('looker/deliveries.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [deliveries_lkml_view]}))


# %%
# TRANSLATE DELIVERY_PEOPLE
delivery_people_lkml_view = to_lkml.model_to_lkml_view(model=model_dict['delivery_people'])

with open('looker/delivery_people.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [delivery_people_lkml_view]}))


# %%
# TRANSLATE CUSTOMERS
customers_lkml_view = to_lkml.model_to_lkml_view(model=model_dict['customers'])

with open('looker/customers.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [customers_lkml_view]}))

# %%
# TRANSLATE LOCATIONS
locations_lkml_view = to_lkml.model_to_lkml_view(model=model_dict['locations'])

with open('looker/locations.view.lkml', 'w') as file:
    file.write(lkml.dump({'views': [locations_lkml_view]}))
# %%
