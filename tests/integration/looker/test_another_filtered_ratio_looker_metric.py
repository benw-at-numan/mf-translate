from tests.integration.helpers import query_metricflow, query_looker, do_query_results_match

def test_another_filtered_ratio_looker_metric(lkr_sdk):

    mf_results = query_metricflow(metrics=['pc_deliveries_with_5_stars'],
                                  group_by=['delivery_person__full_name'],
                                  order_by=['-pc_deliveries_with_5_stars'])

    lkr_results = query_looker(explore='deliveries',
                               fields=['delivery_people.full_name',
                                       'deliveries.pc_deliveries_with_5_stars'],
                               sorts=['-deliveries.pc_deliveries_with_5_stars'],
                               lkr_sdk=lkr_sdk)

    assert do_query_results_match(lkr_results, mf_results)