# `mf-compare-query`

`mf-compare-query` asserts whether results from a MetricFlow query match results from an equivalent Looker query (support for other semantic layers may be added in future).

## Environment Variables
| Variable                       | Usage                                        | Description                                                                                                                                                           |
|--------------------------------|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MF_TRANSLATE_LOOKER_MODEL`    | Required                                     | Defines the LookML model to be queried. Case-sensitive. See [Looker model docs](https://cloud.google.com/looker/docs/lookml-project-files#model_files) for more info. |
| `MF_TRANSLATE_LOOKER_PROJECT`  | Optional, required for `--looker-dev-branch` | Defines the Looker project. Required if including a `--looker-dev-branch` argument. Case-sensitive. See [Looker project docs](https://cloud.google.com/looker/docs/lookml-terms-and-concepts#lookml_project) for more info. |
| `LOOKERSDK_BASE_URL`           | Required                                     | The base URL for the Looker API (e.g., `https://yourcompany.looker.com`). See [Looker SDK docs](https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration) for details. |
| `LOOKERSDK_CLIENT_ID`          | Required                                     | The client ID for Looker API authentication. See [Looker SDK docs](https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration) for details. |
| `LOOKERSDK_CLIENT_SECRET`      | Required                                     | The client secret for Looker API authentication. See [Looker SDK docs](https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration) for details. |

## Arguments
| Argument                      | Usage    | Description                                                                                                                                                                                           |
|-------------------------------|--------- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--to-looker-explore`         | Required | Specify the Looker Explore to query against.                                                                                                                                                          |
| `--metrics`                   | Required | A comma-separated list of metrics to query, e.g., `--metrics bookings,messages`. The metrics must be derived from measures in the same semantic model.                                                |
| `--group-by`                  | Optional | A comma-separated list of dimensions or entities to group by, e.g., `--group-by customer_name,region`.                                                                                                |
| `--where`                     | Optional | SQL-like `WHERE` statement provided in quotes: `--where "condition_statement"`. Example: `--where "{{ Dimension('order_id__revenue') }} > 100 and {{ Dimension('customer_id__region') }} = 'US'"`.    |
| `--looker-filters`            | Optional | A list of Looker filters wrapped in curly braces and quotes: `--looker-filters "{'orders.revenue': '>100', 'customers.region': 'US'}"`.                                                               |
| `--looker-dev-branch`         | Optional | Specify a development branch for Looker comparisons. If not provided, the Looker production environment will be used.                                                                                 |
| `--log-level`                 | Optional | Set the logging level for the tool. Available levels are `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. The default is `INFO`.                                                                      |

## Installation
```bash
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```