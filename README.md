# mf-translate

`mf-translate` converts MetricFlow models and metrics to alternative semantic layers. The idea is to lighten the load of parallel running of two semantic layers by enabling automated synchronisation of definitions. The repo also includes `mf-compare-query` for checking that equivalent queries for different semantic layers return the same result.

## Quickstart

### Install
```sh
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```

### Translate a model
Translate all the dimensions and metrics from the `deliveries` semantic model.
```bash
mf-translate mf-translate --model deliveries --to-looker > looker/deliveries_base.view.lkml
```

### Compare semantic layer queries
Setup Looker credentials:
```bash
export LOOKERSDK_BASE_URL="https://your.looker.instance:19999"
export LOOKERSDK_CLIENT_ID="your_client_id"
export LOOKERSDK_CLIENT_SECRET="your_client_secret"
```

Set target Looker project:
```bash
export MF_TRANSLATE_LOOKER_PROJECT="jaffle_shop"
```

Compare a MetricFlow query results to the equivalent Looker query:
```bash
mf-compare-query --metrics deliveries_count --group-by delivery_person_id__full_name --to-looker
```
Note that the above requires the LookML translations to have been deployed to production.

## Supported Semantic Layers
Currently only Looker is supported as a translation destination but there is potential to expand support to Cube.dev and Lightdash. Also, `mf-translate` only supports one-way translation, it is not possible to translate a LookML model back to MetricFlow for example. Below are the metric types which can currently be translated: -

| Metric Type | Looker | Cube.dev | Lightdash |
|-------------|:------:|:--------:|:---------:|
| simple      | ✅     |          |           |
| ratio       | ✅     |          |           |
| derived     |        |          |           |
| cumulative  |        |          |           |
| conversion  |        |          |           |

Note that only metrics which depend on measures from the same semantic model can be translated to Looker (the same is true for Cube.dev and Lightdash).

