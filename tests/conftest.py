# tests/conftest.py
import os
import pytest
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import looker_sdk
from io import StringIO

load_dotenv()

@pytest.fixture(scope="session")
def bq_client():
    load_dotenv('.env')
    gcloud_project_id = 'fresh-iridium-428713-j5'
    credentials = service_account.Credentials.from_service_account_file(
        'tests/integration/secrets/fresh-iridium-428713-j5-fa15322e7990.json'
    )
    client = bigquery.Client(credentials=credentials, project=gcloud_project_id)
    yield client

@pytest.fixture(scope="session")
def lkr_sdk():
    sdk = looker_sdk.init40("tests/integration/secrets/looker.ini")
    yield sdk

