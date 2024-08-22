view: deliveries {
  sql_table_name: `fresh-iridium-428713-j5`.`jaffle_shop`.`deliveries` ;;

  dimension_group: delivered_at {
    type: time
    timeframes: [
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: delivery_id {
    primary_key: yes
    hidden: yes
  }

  dimension: order_id {
    hidden: yes
  }

  dimension: delivery_person_id {
    hidden: yes
  }

  dimension: delivery_rating {}

  measure: pc_deliveries_with_5_stars_numerator {
    type: count_distinct
    sql: case when (${delivery_rating} = 5)
            then (${TABLE}.delivery_id)
         end ;;
    hidden: yes
  }

  measure: pc_deliveries_with_5_stars_denominator {
    type: count
    hidden: yes
  }

  measure: pc_deliveries_with_5_stars {
    type: number
    label: "Deliveries with 5 stars (%)"
    description: "Percentage of deliveries that received a 5-star rating."
    sql: ${pc_deliveries_with_5_stars_numerator} / nullif(${pc_deliveries_with_5_stars_denominator}, 0) ;;
  }

  measure: delivery_count {
    type: count
  }
}