# `mf-translate`

`mf-translate` converts MetricFlow models and metrics to alternative semantic layers. The idea is to lighten the load of parallel running of two semantic layers by enabling automated synchronisation of definitions. Currently only translation to looker views is supported.

## Environment Variables
```bash
export MF_TRANSLATE_TARGET_WAREHOUSE_TYPE='snowflake'
```
Required, defines the warehouse type which the translated semantic model definitions will run against. Supported values are `snowflake`, `redshift` and `bigquery`.

```bash
export MF_TRANSLATE_DATABASE_TIMEZONE='UTC'
```
Required for translations to Cube. Describes the timezone which the source database stores timestamps in (typically UTC). This is used to provide Cube with a timezoned timestamp - whereas MetricFlow works datetimes without timezones. More information here: -
 - https://cube.dev/docs/guides/recipes/data-modeling/string-time-dimensions
 - https://github.com/dbt-labs/metricflow/issues/733

## Arguments
```bash
--model MODEL_NAME (required): The name of the model which is to be translated.

--to-looker-view VIEW_NAME (optional): The name of the looker view to be created.

--to-cube-cube CUBE_NAME (optional): The name of the Cube cube to be created.
```

## Installation
```bash
pip install git+https://github.com/benw-at-birdie/mf-translate.git
```