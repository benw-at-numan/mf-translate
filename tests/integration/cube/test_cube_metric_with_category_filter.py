from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_cube_metric_with_category_filter():

    mf_results = query_metricflow(metrics=['food_orders'])

    cube_results = query_cube(query={"measures": ["orders.food_orders"]})

    assert do_query_results_match(cube_results, mf_results)