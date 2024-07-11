
view: orders {

  sql_table_name: fresh-iridium-428713-j5.dbt_slt.orders ;;

  dimension: order_id {
    primary_key: yes
  }

  dimension_group: ordered_at {
    type: time
    timeframes: [date]
    sql: ${TABLE}.ordered_at ;;
  }

  measure: order_total {
    label: "Order Total"
    description: "Sum of total order amonunt. Includes tax + revenue."
    type: sum
  }

}
