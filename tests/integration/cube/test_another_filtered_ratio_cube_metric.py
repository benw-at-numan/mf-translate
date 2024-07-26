from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_filtered_ratio_cube_metric():

    mf_results = query_metricflow(metrics=['pc_deliveries_with_5_stars'],
                                  group_by=['delivery_person__full_name'],
                                  order_by=['-pc_deliveries_with_5_stars'])
    
    mf_results['column_2'] = mf_results['column_2'].round(15)

    cube_results = query_cube(query={"measures": ["deliveries.pc_deliveries_with_5_stars"], 
                                     "dimensions": ["delivery_people.full_name"],
                                     "order": { "deliveries.pc_deliveries_with_5_stars": "desc" }})
    
    cube_results['deliveries.pc_deliveries_with_5_stars'] \
        = cube_results['deliveries.pc_deliveries_with_5_stars'].round(15)

    assert do_query_results_match(cube_results, mf_results)