view: orders {
  sql_table_name: `fresh-iridium-428713-j5`.`jaffle_shop`.`orders` ;;

  dimension_group: ordered_at {
    type: time
    timeframes: [
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: order_id {
    primary_key: yes
    hidden: yes
  }

  dimension: location_id {
    hidden: yes
    sql: ${TABLE}.location_id ;;
  }

  dimension: customer_id {
    hidden: yes
    sql: ${TABLE}.customer_id ;;
  }

  dimension: ordered_at_test {
    sql: ${TABLE}.ordered_at ;;
    type: date_time
  }

  dimension: discount_code {}

  dimension: order_total_dim {
    sql: ${TABLE}.order_total ;;
  }

  dimension: is_food_order {}

  dimension: is_drink_order {}

  dimension: is_large_order {
    sql: ${TABLE}.order_total > 20 ;;
  }

  measure: customers_with_orders {
    type: count_distinct
    sql: ${TABLE}.customer_id ;;
    label: "Customers w/ Orders"
    description: "Distict count of customers placing orders"
  }

  measure: new_customer {
    type: count_distinct
    sql: case when (${customers.customer_type}  = 'new')
            then (${TABLE}.customer_id)
         end ;;
    label: "New Customers"
    description: "Unique count of new customers."
  }

  measure: order_total {
    type: sum
    sql: ${TABLE}.order_total ;;
    label: "Order Total"
    description: "Sum of total order amonunt. Includes tax + revenue."
  }

  measure: orders {
    type: sum
    sql: 1 ;;
    label: "Orders"
    description: "Count of orders."
  }

  measure: orders_fill_nulls_with_zero {
    type: sum
    sql: 1 ;;
    label: "Orders (Fill nulls with 0)"
    description: "Example metric colaescing null to zero."
  }

  measure: food_orders {
    type: sum
    sql: case when (${is_food_order} = true)
            then (1)
         end ;;
    label: "Food Orders"
    description: "Count of orders that contain food order items"
  }

  measure: large_orders {
    type: sum
    sql: case when (${is_large_order} = true)
            then (1)
         end ;;
    label: "Large Orders"
    description: "Count of orders with order total over 20."
  }

  measure: pc_drink_orders_for_returning_customers_numerator {
    type: sum
    sql: case when (${is_drink_order} = true)
               and (${customers.customer_type} = 'returning')
            then (1)
         end ;;
    hidden: yes
  }

  measure: pc_drink_orders_for_returning_customers_denominator {
    type: sum
    sql: case when (${customers.customer_type} = 'returning')
            then (1)
         end ;;
    hidden: yes
  }

  measure: pc_drink_orders_for_returning_customers {
    type: number
    label: "Drink orders for returning customers (%)"
    description: "Percentage of orders which are drink orders."
    sql: ${pc_drink_orders_for_returning_customers_numerator} / nullif(${pc_drink_orders_for_returning_customers_denominator}, 0) ;;
  }

  measure: order_cost {
    type: sum
    sql: ${TABLE}.order_cost ;;
    label: "Order Cost"
    description: "Sum of cost for each order item."
  }
}