import pytest
from dotenv import load_dotenv
import looker_sdk
from io import StringIO

load_dotenv()

@pytest.fixture(scope="session")
def setup_looker_sdk():
    sdk = looker_sdk.init40("tests/integration/secrets/looker.ini")
    yield sdk