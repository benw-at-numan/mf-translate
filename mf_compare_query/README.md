# `mf-compare-query`

`mf-compare-query` asserts whether the results from a specified MetricFlow query match the results from an equivalent Looker query (support for other semantic layers may be added in future). The Looker integration supports querying multiple metrics, grouping, and ordering by various dimensions and entities, as wells as comparing results to a specific Looker development branch.

## Environment Variables
```bash
export MF_TRANSLATE_LOOKER_MODEL='your_looker_model'
```
Required, defines the LookML model to be queried. Case-sensitive. See [cloud.google.com/looker/docs/lookml-project-files#model_files](https://cloud.google.com/looker/docs/lookml-project-files#model_files) for more information on Looker models.

```bash
export MF_TRANSLATE_LOOKER_PROJECT='your_looker_project'
```
Optional, required if including a `--looker-dev-branch` argument, see section below. Case-sensitive. See [cloud.google.com/looker/docs/lookml-terms-and-concepts#lookml_project](https://cloud.google.com/looker/docs/lookml-terms-and-concepts#lookml_project) for more information on Looker projects.

## Arguments
```bash
--to-looker-explore EXPLORE_NAME (required): Specify the Looker Explore to query against. 

--metrics SEQUENCE (required): A comma-separated list of metrics to query, e.g., --metrics bookings,messages. The metrics must be derived from measures in the same semantic model.

--group-by SEQUENCE (optional): A comma-separated list of dimensions or entities to group by, e.g., --group-by customer_name,region.

--order-by SEQUENCE (optional): A comma-separated list of dimensions or entities to order the results by, e.g., --order-by customer_name,-region. Use - to specify descending order for a dimension.

--where WHERE STRING (optional): SQL-like where statement provided in wrapped quotes: --where "condition_statement" - e.g. --where "{{ Dimension('order_id__revenue') }} > 100 and {{ Dimension('customer_id__region') }}  = 'US'". Note that a corresponding --looker-filters argument must also be provided to apply like for like filtering when comparing against Looker.

--looker-filters FILTER STRING (optional): List of Looker filters wrapped in curly braces and quotes:  --looker-filters "{'orders.revenue': '>100', 'customers.region': 'US'}".

--looker-dev-branch BRANCH_NAME (optional): Specify a development branch for Looker comparisons. If not provided, the Looker production environment will be used.

--log-level LEVEL (optional): Set the logging level for the tool. Available levels are DEBUG, INFO, WARNING, ERROR, CRITICAL. The default is INFO.
 ```

## Installation
```bash
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```