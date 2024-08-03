from tests.integration.helpers import query_metricflow, query_looker, do_query_results_match

def test_simple_looker_metric(setup_dbt, setup_looker_sdk):

    mf_results = query_metricflow(metrics=['order_total'])
    lkr_results = query_looker(explore='orders',
                               fields=['orders.order_total'],
                               lkr_sdk=setup_looker_sdk)

    assert do_query_results_match(lkr_results, mf_results)


def test_looker_metric_with_category_filter(setup_looker_sdk):

    mf_results = query_metricflow(metrics=['food_orders'])
    lkr_results = query_looker(explore='orders',
                               fields=['orders.food_orders'],
                               lkr_sdk=setup_looker_sdk)

    assert do_query_results_match(lkr_results, mf_results)


def test_another_looker_metric_with_category_filter(setup_looker_sdk):

    mf_results = query_metricflow(metrics=['large_orders'])
    lkr_results = query_looker(explore='orders',
                               fields=['orders.large_orders'],
                               lkr_sdk=setup_looker_sdk)

    assert do_query_results_match(lkr_results, mf_results)


def test_filtered_ratio_looker_metric(setup_dbt, setup_looker_sdk):

    mf_results = query_metricflow(metrics=['pc_drink_orders_for_returning_customers'],
                                  group_by=['location__location_name'],
                                  order_by=['location__location_name'])

    lkr_results = query_looker(explore='orders',
                               fields=['locations.location_name',
                                       'orders.pc_drink_orders_for_returning_customers'],
                               sorts=['locations.location_name'],
                               lkr_sdk=setup_looker_sdk)

    assert do_query_results_match(lkr_results, mf_results)


def test_another_filtered_ratio_looker_metric(setup_dbt, setup_looker_sdk):

    mf_results = query_metricflow(metrics=['pc_deliveries_with_5_stars'],
                                  group_by=['delivery_person_id__full_name'],
                                  order_by=['-pc_deliveries_with_5_stars'])

    lkr_results = query_looker(explore='deliveries',
                               fields=['delivery_people.full_name',
                                       'deliveries.pc_deliveries_with_5_stars'],
                               sorts=['-deliveries.pc_deliveries_with_5_stars'],
                               lkr_sdk=setup_looker_sdk)

    assert do_query_results_match(lkr_results, mf_results)