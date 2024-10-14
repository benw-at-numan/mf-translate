connection: "jaffle_shop"

include: "*.view.lkml"

access_grant: has_jaffle_shop_dev_access {
  user_attribute: is_jaffle_shop_dev
  allowed_values: ["Yes"]
}

explore: orders {
  required_access_grants: [has_jaffle_shop_dev_access]

  join: customers {
    sql_on: ${orders.customer_id} = ${customers.customer_id} ;;
  }

  join: locations {
    sql_on: ${orders.location_id} = ${locations.location_id} ;;
  }
}

explore: deliveries {
  required_access_grants: [has_jaffle_shop_dev_access]

  join: delivery_people {
    sql_on: ${deliveries.delivery_person_id} = ${delivery_people.delivery_person_id} ;;
  }

  join: orders {
    sql_on: ${deliveries.order_id} = ${orders.order_id} ;;
  }

  join: customers {
    sql_on: ${orders.customer_id} = ${customers.customer_id} ;;
  }
}
