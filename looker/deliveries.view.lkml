view: deliveries {

  sql_table_name: fresh-iridium-428713-j5.jaffle_shop.deliveries ;;

  dimension: delivery_id {
    primary_key: yes
  }

  dimension: delivery_person_id {
    hidden: yes
  }

  dimension: order_id {
    hidden: yes
  }

  dimension: delivery_rating {}

  dimension_group: delivered_at {
    type: time
    timeframes: [date]
    sql: CAST(${TABLE}.delivered_at as DATETIME) ;;
  }

  measure: delivery_count {
    type: count
  }

  # PC_DELIVERIES_WITH_5_STARS
  measure: five_star_deliveries_count {
    hidden: yes
    type: count_distinct
    sql:
      case when (${delivery_rating} = 5) and (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
        then ${delivery_id}
      end ;;
  }
  measure: pc_deliveries_with_5_stars_denominator {
    hidden: yes
    type: count_distinct
    sql:
      case when (coalesce(${orders.discount_code}, 'NO_DISCOUNT') != 'STAFF_ORDER')
        then ${delivery_id}
      end ;;
  }
  measure: pc_deliveries_with_5_stars {
    label: "Deliveries with 5 stars (%)"
    description: "Percentage of deliveries that received a 5-star rating."
    type: number
    sql: ${five_star_deliveries_count} / nullif(${pc_deliveries_with_5_stars_denominator}, 0) ;;
  }

}
