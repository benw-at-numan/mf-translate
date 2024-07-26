from tests.integration.helpers import query_metricflow, query_looker, do_query_results_match

def test_filtered_ratio_looker_metric(lkr_sdk):

    mf_results = query_metricflow(metrics=['pc_drink_orders_for_returning_customers'],
                                  group_by=['location__location_name'],
                                  order_by=['location__location_name'])

    lkr_results = query_looker(explore='orders',
                               fields=['locations.location_name',
                                       'orders.pc_drink_orders_for_returning_customers'],
                               sorts=['locations.location_name'],
                               lkr_sdk=lkr_sdk)

    assert do_query_results_match(lkr_results, mf_results)