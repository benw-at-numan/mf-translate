import argparse
import sys
import subprocess
import json
import lkml
from ruamel.yaml import YAML
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S'
)

from . import to_looker, to_cube

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
    parser.add_argument('--to-looker-view', type=str, required=False, help='Name of the Looker view to be created.', metavar='STRING')
    parser.add_argument('--to-cube-cube', type=str, required=False, help='Name of the Cube cube to be created.', metavar='STRING')

    args = parser.parse_args()

    if (not args.to_looker_view) and (not args.to_cube_cube):
        logging.error("No translation target specified. Please specify either --to-looker-view or --to-cube-cube.")
        sys.exit(1)

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

        semantic_model = model_dict.get(args.model)
        if not semantic_model:
            logging.error(f"Model `{args.model}` not found in target/semantic_manifest.json.")
            sys.exit(1)

        to_looker.set_manifests(metricflow_semantic_manifest=semantic_manifest,
                                dbt_manifest=manifest)
        lkml_view = to_looker.model_to_lkml_view(model=model_dict[args.model], view_name=args.to_looker_view)
        print(lkml.dump({'views': [lkml_view]}))

        logging.info(f"Translated {args.model} semantic model to LookML view {args.to_looker_view}.")

    elif args.to_cube_cube:

        semantic_model = model_dict.get(args.model)
        if not semantic_model:
            logging.error(f"Model `{args.model}` not found in target/semantic_manifest.json.")
            sys.exit(1)

        to_cube.set_manifests(metricflow_semantic_manifest=semantic_manifest,
                              dbt_manifest=manifest)
        cube = to_cube.model_to_cube_cube(model=model_dict[args.model], cube_name=args.to_cube_cube)
        print(YAML().dump({"cubes": [cube]}, sys.stdout))

        logging.info(f"Translated {args.model} semantic model to Cube cube {args.to_cube_cube}.")

if __name__ == '__main__':
    main()