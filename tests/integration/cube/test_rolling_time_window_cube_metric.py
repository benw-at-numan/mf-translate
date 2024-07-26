import pandas as pd
from tests.integration.helpers import query_metricflow, query_cube, do_query_results_match

def test_rolling_time_window_cube_metric():

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