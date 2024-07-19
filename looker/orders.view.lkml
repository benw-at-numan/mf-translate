
view: orders {

  sql_table_name: fresh-iridium-428713-j5.jaffle_shop.orders ;;

  dimension: order_id {
    primary_key: yes
  }

  dimension: customer_id {}

  dimension: location_id {}

  dimension_group: ordered_at {
    type: time
    timeframes: [date]
    sql: CAST(${TABLE}.ordered_at as DATETIME) ;;
  }

  dimension: is_food_order {}

  dimension: is_drink_order {}

  measure: order_total {
    label: "Order Total"
    description: "Sum of total order amonunt. Includes tax + revenue."
    type: sum
  }

  measure: orders {
    description: "Count of orders"
    label: "Orders"
    type: sum
    sql: (1) ;;
  }

  measure: food_orders {
    description: "Count of orders that contain food order items"
    label: "Food Orders"
    type: number
    sql:
      sum(
        case when (${is_food_order} = true)
          then (1)
        end
      ) ;;
  }

  measure: pc_drink_orders_for_returning_customers_numerator {
    hidden: yes
    type: number
    sql:
      sum(
        case when (${is_drink_order} = true) and (${customers.customer_type} = 'returning')
          then (1)
        end
      ) ;;
  }
  measure: pc_drink_orders_for_returning_customers_denominator {
    hidden: yes
    type: number
    sql:
      sum(
        case when (${customers.customer_type} = 'returning')
          then (1)
        end
      ) ;;
  }
  measure: pc_drink_orders_for_returning_customers {
    label: "Drink orders for returning customers (%)"
    description: "Percentage of orders which are drink orders."
    type: number
    sql: ${pc_drink_orders_for_returning_customers_numerator} / nullif(${pc_drink_orders_for_returning_customers_denominator}, 0) ;;
  }
}
