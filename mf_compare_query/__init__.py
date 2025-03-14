import sys
import argparse
import subprocess
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S'
)
import json
import ast

from . import to_looker

def parse_csv_str(value):
    return value.split(',')

def parse_dict(dict_string):
    try:
        return ast.literal_eval(dict_string)
    except (SyntaxError, ValueError):
        logging.error("Invalid format for dictionary string. Example: \"{'orders.revenue': '>100', 'customers.region': 'US'}\"")
        sys.exit(1)

def main():

    # LOAD ARGUMENTS
    parser = argparse.ArgumentParser(description='Asserts if the results for the specified MetricFlow query match the results from an equivalent Looker/Cube/Lightdash query.')

    parser.add_argument('--metrics', type=parse_csv_str, required=True, metavar='SEQUENCE',
                        help='Comma-separated list of metrics, for example, --metrics bookings,messages. Note that the listed metrics should be derived from measures in the same semantic model.')

    parser.add_argument('--group-by', type=parse_csv_str, required=False, metavar='SEQUENCE',
                        help='List of dimensions/entities to group by, e.g. --group-by customer_name,region.')

    parser.add_argument('--where', type=str, required=False, metavar='STRING',
                        help='SQL-like where statement provided as a string and wrapped in quotes: --where "condition_statement" - e.g. --where "{{ Dimension(\'order_id__revenue\') }} > 100 and {{ Dimension(\'customer_id__region\') }}  = \'US\'". Note that a corresponding `--looker-filters` argument must be provided to apply like for like filtering when comparing against Looker.')

    parser.add_argument('--to-looker-explore', type=str, required=True,
                        help='Compare the query results to the specified Looker Explore (rather than inferring the Explore from the --metrics input).')

    parser.add_argument('--looker-filters', type=parse_dict, required=False, metavar='STRING',
                        help='List of Looker filters wrapped in curly braces and quotes:  --looker-filters "{\'orders.revenue\': \'>100\', \'customers.region\': \'US\'}".')

    parser.add_argument('--looker-dev-branch', type=str, required=False,
                        help='The development git branch to use when querying Looker. If not specified, the production environment will be used. Note that MF_TRANSLATE_LOOKER_PROJECT environment variable must be set.')

    parser.add_argument('--log-level', type=str, required=False, default='INFO',
                        help='Set the logging level, options are DEBUG, INFO, WARNING, ERROR, CRITICAL.')

    args = parser.parse_args()

    if args.log_level:
        logging.getLogger().setLevel(args.log_level)


    # PARSE DBT PROJECT
    logging.info("Parsing dbt project...")
    result = subprocess.run(['dbt', 'parse'], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Project could not be parsed.\n"
                      f"dbt log:---\n{result.stdout.strip()}\n---"
        )
        sys.exit(1)
    logging.info("...finished parsing project.")

    semantic_manifest = {}
    try:
        with open(f'target/semantic_manifest.json') as f:
            semantic_manifest = json.load(f)
    except FileNotFoundError:
        logging.error(f"The file {f'target/semantic_manifest.json'} does not exist.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"The file {f'target/semantic_manifest.json'} is not a valid JSON file.")
        sys.exit(1)

    to_looker.set_semantic_manifest(semantic_manifest)
    logging.debug(f"Parsed dbt project.")


    # QUERY METRICFLOW AND LOOKER
    mf_results = to_looker.query_metricflow(metrics=args.metrics, group_by=args.group_by,
                                            where=args.where)
    logging.info(f"MetricFlow query returned {mf_results.shape[0]} rows.")
    lkr_results = to_looker.query_looker(metrics=args.metrics, group_by=args.group_by,
                                         filters=args.looker_filters,
                                         explore=args.to_looker_explore,
                                         dev_branch=args.looker_dev_branch)
    logging.info(f"Looker query returned {lkr_results.shape[0]} rows.")

    mf_results.columns = lkr_results.columns # MF does not return column names so overwrite them with Looker's.
    if not to_looker.do_query_results_match(metricflow_results=mf_results,
                                            looker_results=lkr_results):
        sys.exit(1)

if __name__ == '__main__':
    main()