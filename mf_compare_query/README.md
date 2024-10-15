# `mf-compare-query`

`mf-compare-query` asserts whether the results from a specified MetricFlow query match the results from an equivalent Looker query (support for other semantic layers may be added in future). The Looker integration supports querying multiple metrics, grouping, and ordering by various dimensions and entities, as wells as comparing results to a specific Looker development branch.

## Environment Variables
```bash
export MF_TRANSLATE_LOOKER_MODEL='your_looker_model'
```
Required, defines the LookML .model to be queried.

```bash
export MF_TRANSLATE_LOOKER_PROJECT='your_looker_project'
```
Optional, required if including a `--to-looker-dev-branch` argument, see section below. Case-sensitive.

## Arguments
```bash
--metrics SEQUENCE (required): A comma-separated list of metrics to query, e.g., --metrics bookings,messages. The metrics must be derived from measures in the same semantic model.

--group-by SEQUENCE (optional): A comma-separated list of dimensions or entities to group by, e.g., --group-by customer_name,region.

--order-by SEQUENCE (optional): A comma-separated list of dimensions or entities to order the results by, e.g., --order-by customer_name,-region. Use - to specify descending order for a dimension.

--to-looker (optional): Add this flag to compare the MetricFlow query results to Looker.

--to-looker-explore NAME (optional): Specify the Looker Explore to query for comparison. If not provided, the Explore will be inferred from the --metrics input.

--to-looker-dev-branch BRANCH_NAME (optional): Specify a development branch for Looker comparisons. If not provided, the Looker production environment will be used.

--log-level LEVEL (optional): Set the logging level for the tool. Available levels are DEBUG, INFO, WARNING, ERROR, CRITICAL. The default is INFO.
 ```

## Installation
```bash
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```