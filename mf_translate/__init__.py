import argparse
import sys
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
        logging.error(f"The file {file_path} does not exist.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"The file {file_path} is not a valid JSON file.")
        sys.exit(1)


def main():

    parser = argparse.ArgumentParser(description='Converts MetricFlow model definitions to other semantic layer dialects. Currently, only Looker LookML is supported.')
    parser.add_argument('--model', type=str, required=True, help='Name of the MetricFlow semantic model to be translated.', metavar='STRING')
    parser.add_argument('--to-looker-view', type=str, required=True, help='Name of the Looker view to be created.', metavar='STRING')

    args = parser.parse_args()

    logging.info("Parsing dbt project...")
    result = subprocess.run(['dbt', 'parse', '--no-partial-parse'], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Project could not be parsed.\n"
                      f"dbt log:---\n{result.stdout.strip()}\n---"
        )
        sys.exit(1)
    logging.info("...finished parsing project.")

    manifest = load_json_file('target/manifest.json')
    semantic_manifest = load_json_file('target/semantic_manifest.json')

    model_dict = {model['name']: model for model in semantic_manifest['semantic_models']}

    if args.to_looker_view:
        to_looker.set_manifests(metricflow_semantic_manifest=semantic_manifest,
                                dbt_manifest=manifest)
        lkml_view = to_looker.model_to_lkml_view(model=model_dict[args.model], view_name=args.to_looker_view)
        print(lkml.dump({'views': [lkml_view]}))

if __name__ == '__main__':
    main()