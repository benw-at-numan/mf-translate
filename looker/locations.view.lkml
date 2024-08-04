view: locations {
  sql_table_name: `fresh-iridium-428713-j5`.`jaffle_shop`.`stg_locations` ;;

  dimension_group: opened_at {
    sql: opened_at ;;
    type: time
    timeframes: [
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: location {
    primary_key: Yes
    hidden: Yes
    sql: location_id ;;
  }

  dimension: location_name {}
}