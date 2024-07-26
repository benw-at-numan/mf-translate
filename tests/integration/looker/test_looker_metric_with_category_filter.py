from tests.integration.helpers import query_metricflow, query_looker, do_query_results_match

def test_looker_metric_with_category_filter(lkr_sdk):

    mf_results = query_metricflow(metrics=['food_orders'])

    lkr_results = query_looker(explore='orders',
                               fields=['orders.food_orders'],
                               lkr_sdk=lkr_sdk)

    assert do_query_results_match(lkr_results, mf_results)