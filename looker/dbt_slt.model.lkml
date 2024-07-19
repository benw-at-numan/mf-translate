connection: "dbt_slt"

include: "*.view.lkml"

access_grant: has_dbt_slt_dev_access {
  user_attribute: is_dbt_slt_dev
  allowed_values: ["Yes"]
}

explore: orders {
  required_access_grants: [has_dbt_slt_dev_access]

  join: customers {
    sql_on: ${orders.customer_id} = ${customers.customer_id} ;;
  }

  join: locations {
    sql_on: ${orders.location_id} = ${locations.location_id} ;;
  }
}
