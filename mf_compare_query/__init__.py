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

from . import to_looker

def parse_csv_str(value):
    return value.split(',')

def main():

    # LOAD ARGUMENTS
    parser = argparse.ArgumentParser(description='Asserts if the results for the specified MetricFlow query match the results from an equivalent Looker/Cube/Lightdash query.')

    parser.add_argument('--metrics', type=parse_csv_str, required=True, metavar='SEQUENCE',
                        help='Comma-separated list of metrics, for example, --metrics bookings,messages')
    parser.add_argument('--group-by', type=parse_csv_str, required=False, metavar='SEQUENCE',
                        help='List of dimensions/entities to group by, e.g. --group-by ds,org')
    parser.add_argument('--order-by', type=parse_csv_str, required=False, metavar='SEQUENCE',
                        help='List of dimensions/entities to order by, e.g. --order-by ds,-org')
    parser.add_argument('--to-looker', action='store_true',
                        help='Compare the query results to Looker.')
    args = parser.parse_args()

    if not args.to_looker:
        raise ValueError("Only comparisons to Looker are supported at the moment.")

    logging.info(f"Comparing MetricFlow query results to Looker...")


    # PARSE DBT PROJECT
    result = subprocess.run(['dbt', 'parse'], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error occurred whilst parsing DBT project: {result.stderr}")

    semantic_manifest = {}
    try:
        with open(f'target/semantic_manifest.json') as f:
            semantic_manifest = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {f'target/semantic_manifest.json'} does not exist.")
    except json.JSONDecodeError:
        raise ValueError(f"The file {f'target/semantic_manifest.json'} is not a valid JSON file.")

    to_looker.set_semantic_manifest(semantic_manifest)
    logging.info(f"Parsed dbt project.")


    # QUERY METRICFLOW AND LOOKER
    mf_results = to_looker.query_metricflow(metrics=args.metrics, group_by=args.group_by, order_by=args.order_by)
    logging.info(f"MetricFlow query returned {mf_results.shape[0]} rows.")
    lkr_results = to_looker.query_looker(metrics=args.metrics, group_by=args.group_by, order_by=args.order_by)
    logging.info(f"Looker query returned {lkr_results.shape[0]} rows.")

    mf_results.columns = lkr_results.columns # MF does not return column names so overwrite them with Looker's.
    if not to_looker.do_query_results_match(df1=mf_results, 
                                            df2=lkr_results):
        sys.exit(1)

if __name__ == '__main__':
    main()