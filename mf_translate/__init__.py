import argparse
import os
import subprocess
import json
import lkml
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S'
)

from . import to_looker

def load_json_file(file_path):
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except json.JSONDecodeError:
        raise ValueError(f"The file {file_path} is not a valid JSON file.")


def main():

    parser = argparse.ArgumentParser(description='mf-translate converts MetricFlow model definitions to alternative semantic layers.')
    parser.add_argument('--model', required=True, help='Specify the model to be translated name')
    parser.add_argument('--to-looker', action='store_true', help='Convert to Looker view')

    args = parser.parse_args()

    if not (args.to_looker):
        raise ValueError("Only translations to Looker are supported at the moment.")

    result = subprocess.run(['dbt', 'parse'], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Dbt project could not be parsed: {result.stderr}")

    manifest = load_json_file('target/manifest.json')
    semantic_manifest = load_json_file('target/semantic_manifest.json')

    model_dict = {model['name']: model for model in semantic_manifest['semantic_models']}

    if args.to_looker:
        to_looker.set_manifests(metricflow_semantic_manifest=semantic_manifest,
                                dbt_manifest=manifest)
        lkml_view = to_looker.model_to_lkml_view(model=model_dict[args.model])
        print(lkml.dump({'views': [lkml_view]}))

if __name__ == '__main__':
    main()