from tests.integration.helpers import query_metricflow, query_looker, do_query_results_match

def test_simple_looker_metric(lkr_sdk):

    mf_results = query_metricflow(metrics=['order_total'])

    lkr_results = query_looker(explore='orders',
                               fields=['orders.order_total'],
                               lkr_sdk=lkr_sdk)

    assert do_query_results_match(lkr_results, mf_results)