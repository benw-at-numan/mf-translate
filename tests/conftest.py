import pytest
from dotenv import load_dotenv
import looker_sdk
import subprocess
from pathlib import Path


@pytest.fixture(scope="session")
def setup_dbt():

    # Load DBT PROFILE DIR
    load_dotenv()

    result = subprocess.run(['dbt', 'parse', '--no-partial-parse'], capture_output=True, text=True, cwd='dbt/')
    if result.returncode != 0:
        raise RuntimeError(f"Dbt project could not be parsed: {result.stderr}")
    
    yield

@pytest.fixture(scope="session")
def setup_looker_sdk():
    sdk = looker_sdk.init40("tests/integration/secrets/looker.ini")
    yield sdk


@pytest.fixture
def test_dir(request):
    return Path(request.fspath).parent.resolve()