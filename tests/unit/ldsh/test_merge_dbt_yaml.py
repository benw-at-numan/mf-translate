from ruamel.yaml import YAML
import mf_translate.to_ldsh as to_ldsh

def test_merge_dbt_yaml(test_dir):

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 4096

    # Read the content of the YAML files
    with open(test_dir / 'source_dbt.yml', 'r') as source_file:
        source_content = source_file.read()

    with open(test_dir / 'update_dbt.yml', 'r') as update_file:
        update_content = update_file.read()

    with open(test_dir / 'merged_dbt.yml', 'r') as expected_file:
        expected_content = expected_file.read()

    # Perform the merge
    merged_data = to_ldsh.merge_dbt_yaml(source_content, update_content)

    # Load the expected merged YAML
    expected_data = yaml.load(expected_content)

    # Assert that the merged data matches the expected data
    assert merged_data == expected_data, "Merged YAML does not match the expected result."