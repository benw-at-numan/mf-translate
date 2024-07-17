
view: orders {

  sql_table_name: fresh-iridium-428713-j5.jaffle_shop.orders ;;

  dimension: order_id {
    primary_key: yes
  }

  dimension_group: ordered_at {
    type: time
    timeframes: [date]
    sql: ${TABLE}.ordered_at ;;
  }

  dimension: is_food_order {}

  measure: order_total {
    label: "Order Total"
    description: "Sum of total order amonunt. Includes tax + revenue."
    type: sum
  }

  measure: food_orders {
    description: "Count of orders that contain food order items"
    label: "Food Orders"
    type: number
    sql:
      sum(
        case  when (${is_food_order} = true)
          then (1)
        end
      ) ;;
  }
}
