view: delivery_people {
  sql_table_name: `fresh-iridium-428713-j5`.`jaffle_shop`.`delivery_people` ;;

  dimension: delivery_person_id {
    primary_key: yes
    hidden: yes
  }

  dimension: full_name {}
}