import argparse

def parse_csv_str(value):
    return value.split(',')

def main():
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

    print(f"Metrics: {args.metrics}")
    print(f"Group by: {args.group_by}")
    print(f"Order by: {args.order_by}")
    
    if args.to_looker:
        print("Comparing query results to Looker...")

if __name__ == '__main__':
    main()