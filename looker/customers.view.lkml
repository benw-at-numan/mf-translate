view: customers {
  sql_table_name: `fresh-iridium-428713-j5`.`jaffle_shop`.`customers` ;;

  dimension_group: first_ordered_at {
    type: time
    timeframes: [
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension_group: last_ordered_at {
    type: time
    timeframes: [
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: customer_id {
    primary_key: yes
    hidden: yes
  }

  dimension: customer_name {}

  dimension: customer_type {}
}