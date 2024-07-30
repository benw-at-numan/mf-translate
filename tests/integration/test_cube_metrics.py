import pandas as pd
from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_simple_looker_metric(setup_dbt):

    mf_results = query_metricflow(metrics=['order_total'])
    cube_results = query_cube(query={"measures": ["orders.order_total"]})

    assert do_query_results_match(cube_results, mf_results)


def test_cube_metric_with_category_filter(setup_dbt):

    mf_results = query_metricflow(metrics=['food_orders'])
    cube_results = query_cube(query={"measures": ["orders.food_orders"]})

    assert do_query_results_match(cube_results, mf_results)


def test_another_cube_metric_with_category_filter(setup_dbt):

    mf_results = query_metricflow(metrics=['large_orders'])
    cube_results = query_cube(query={"measures": ["orders.large_orders"]})

    assert do_query_results_match(cube_results, mf_results)


def test_filtered_ratio_cube_metric(setup_dbt):

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


def test_another_filtered_ratio_cube_metric(setup_dbt):

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


def test_rolling_time_window_cube_metric(setup_dbt):

    mf_results = query_metricflow(metrics=['orders_last_7_days'],
                                  group_by=['metric_time'],
                                  order_by=['metric_time'],
                                  start_time='2016-09-01',
                                  end_time='2016-09-30')

    cube_results = query_cube(query={"measures": ["orders.orders_last_7_days"], 
                                     "timeDimensions": [{"dimension": "orders.ordered_at",
                                                         "granularity": "day",
                                                         "dateRange": [  "2016-09-01",  "2016-09-30"] }],  
                                     "order": { "orders.ordered_at": "asc" }})    

    cube_results = cube_results.drop(columns=['orders.ordered_at'])
    cube_results['orders.ordered_at.day'] = pd.to_datetime(cube_results['orders.ordered_at.day'])

    assert do_query_results_match(cube_results[['orders.ordered_at.day', 'orders.orders_last_7_days']], mf_results)