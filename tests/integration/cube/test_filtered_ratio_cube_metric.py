from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_filtered_ratio_cube_metric():

    mf_results = query_metricflow(metrics=['pc_drink_orders_for_returning_customers'],
                                  group_by=['location__location_name'],
                                  order_by=['location__location_name'])
    
    mf_results['column_2'] = mf_results['column_2'].round(16)

    cube_results = query_cube(query={"measures": ["orders.pc_drink_orders_for_returning_customers"],
                                     "dimensions": ["locations.location_name"],
                                     "order": { "locations.location_name": "asc" }})
    
    cube_results['orders.pc_drink_orders_for_returning_customers'] \
        = cube_results['orders.pc_drink_orders_for_returning_customers'].round(16)

    assert do_query_results_match(cube_results, mf_results)