# tests/integration/test_simple_metric.py
from tests.integration.helpers import generate_metricflow_results, generate_cube_results, do_query_results_match

def test_simple_metric(bq_client, lkr_sdk):
    generate_metricflow_results(mf_command='mf query --metrics order_total', results_table='simple_metric', bq_client=bq_client, gcloud_project_id='fresh-iridium-428713-j5')
    generate_cube_results(query={"measures": ["orders.order_total"]}, results_table='simple_metric', bq_client=bq_client, gcloud_project_id='fresh-iridium-428713-j5')
    assert do_query_results_match('select * from mf_query_results.simple_metric', 'select * from cube_query_results.simple_metric', bq_client)