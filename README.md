# mf-translate

`mf-translate` converts MetricFlow models and metrics to alternative semantic layers. The idea is to lighten the load of parallel running of two semantic layers by enabling automated synchronisation of definitions. The repo also includes `mf-compare-query` for checking that equivalent queries against different semantic layers return the same result.

## Quickstart

### 1. Install
```sh
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```

### 2. Translate a model
Translate all the dimensions and metrics from the `deliveries` semantic model.
```bash
cd your_dbt_project_directory/
mf-translate --model deliveries --to-looker-view deliveries_base > looker/deliveries_base.view.lkml
```

### 3. Compare semantic layer queries
Setup Looker credentials:
```bash
export LOOKERSDK_BASE_URL="https://your.looker.instance:19999"
export LOOKERSDK_CLIENT_ID="your_client_id"
export LOOKERSDK_CLIENT_SECRET="your_client_secret"
```

Set target Looker model:
```bash
export MF_TRANSLATE_LOOKER_MODEL="jaffle_shop"
```
Note that the model name is case sensitive.

Compare a MetricFlow query results to the equivalent Looker query:
```bash
mf-compare-query --to-looker-explore deliveries --metrics deliveries_count --group-by delivery_person_id__full_name
```
See the `mf-compare-query` [readme](mf_compare_query/README.md) for more information.

## Supported Semantic Layers
Currently only Looker is supported as a translation destination but there is potential to expand support to Cube.dev and Lightdash. Also, `mf-translate` only supports one-way translation, it is not possible to translate a LookML model back to MetricFlow for example. Below are the metric types which can currently be translated: -

| Metric Type | Looker | Cube.dev | Lightdash |
|-------------|:------:|:--------:|:---------:|
| simple      | ✅     |          |           |
| ratio       | ✅     |          |           |
| derived     |        |          |           |
| cumulative  |        |          |           |
| conversion  |        |          |           |

