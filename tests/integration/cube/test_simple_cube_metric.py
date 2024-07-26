from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_simple_looker_metric():

    mf_results = query_metricflow(metrics=['order_total'])

    cube_results = query_cube(query={"measures": ["orders.order_total"]})

    assert do_query_results_match(cube_results, mf_results)