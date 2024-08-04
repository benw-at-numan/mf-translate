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
    primary_key: Yes
    hidden: Yes
  }

  dimension: order_id {
    hidden: Yes
  }

  dimension: delivery_person_id {
    hidden: Yes
  }

  dimension: delivery_rating {}

  measure: pc_deliveries_with_5_stars_numerator {
    type: count_distinct
    sql: case when (${deliveries.delivery_rating} = 5)
               and (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
            then (delivery_id)
         end ;;
    hidden: Yes
  }

  measure: pc_deliveries_with_5_stars_denominator {
    type: count_distinct
    sql: case when (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
            then (delivery_id)
         end ;;
    hidden: Yes
  }

  measure: pc_deliveries_with_5_stars {
    type: number
    label: "Deliveries with 5 stars (%)"
    description: "Percentage of deliveries that received a 5-star rating."
    sql: ${pc_deliveries_with_5_stars_numerator} / nullif(${pc_deliveries_with_5_stars_denominator}, 0) ;;
  }

  measure: delivery_count {
    type: count
    label: "delivery_count"
  }
}