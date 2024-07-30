import mf_translate.to_cube as to_cube

def test_primary_key_dimension():

    assert False


def test_time_dimension_without_timezone(monkeypatch):

    mf_created_at = {
        "name": "created_at",
        "type": "time",
        "label": "Time of creation",
        "description": "Time of creation, without timezone",
        "expr": "ts_created",
        "type_params": {
            "time_granularity": "day"
        }
    }

    monkeypatch.delenv('MF_TRANSLATE_TO_CUBE__TIMEZONE_FOR_TIME_DIMENSIONS')

    cube_created_at = to_cube.dimension_to_cube(mf_created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "(ts_created)"


def test_time_dimension_with_timezone(monkeypatch):

    mf_created_at = {
        "name": "created_at",
        "type": "time",
        "label": "Time of creation",
        "description": "Time of creation, without timezone",
        "expr": "ts_created",
        "type_params": {
            "time_granularity": "day"
        }
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'bigquery')
    monkeypatch.setenv('MF_TRANSLATE_TO_CUBE__TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(mf_created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["title"] == "Time of creation"
    assert "label" not in cube_created_at
    assert cube_created_at["description"] == "Time of creation, without timezone"
    assert cube_created_at["sql"] == "TIMESTAMP(ts_created, 'America/Los_Angeles')"

def test_time_dimension_with_timezone_2(monkeypatch):

    mf_created_at = {
        "name": "created_at",
        "type": "time",
        "expr": "ts_created",
    }

    monkeypatch.setenv('MF_TRANSLATE__TARGET_DATABASE', 'snowflake')
    monkeypatch.setenv('MF_TRANSLATE_TO_CUBE__TIMEZONE_FOR_TIME_DIMENSIONS', 'America/Los_Angeles')

    cube_created_at = to_cube.dimension_to_cube(mf_created_at)

    assert cube_created_at["name"] == "created_at"
    assert cube_created_at["type"] == "time"
    assert cube_created_at["sql"] == "CONVERT_TIMEZONE('America/Los_Angeles', ts_created)"