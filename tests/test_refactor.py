import tempfile
from pathlib import Path

import pytest
from yaml import safe_load

from dbt_cleanup.refactor import (
    SQLRefactorResult,
    YMLRefactorResult,
    changeset_all_sql_yml_files,
    changeset_refactor_yml,
    load_yaml_check_duplicates,
    output_yaml,
    remove_unmatched_endings,
)


@pytest.fixture
def temp_project_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        project_dir = Path(tmpdirname)

        # Create dbt_project.yml
        project_dir.joinpath("dbt_project.yml").write_text("""
model-paths: ["models"]
""")

        # Create models directory
        models_dir = project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        yield project_dir


@pytest.fixture
def schema_yml_with_duplicates():
    return """
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    description: "A starter dbt model second"
    desciption: "A starter dbt model third"
    zorderr: 30
    meta:
      abc: 123
    config:
      meta:
        def: 456
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null

  - name: my_second_dbt_model
    # this is a comment
    description: "A starter dbt model" # and a comment
    desciption: "A starter dbt model" # and a commenttt
    # this is a comment
    materialized: table # yep
    config:
      meta: 
       abc: 123
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
"""


@pytest.fixture
def schema_yml_with_config_fields():
    return """
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    materialized: table
    database: my_db
    schema: my_schema
    meta:
      abc: 123
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
"""


class TestUnmatchedEndingsRemoval:
    """Tests for remove_unmatched_endings function"""

    def test_basic_unmatched_endmacro(self):
        sql_content = """
        select *
        from my_table
        {% endmacro %}
        where x = 1
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endmacro %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endmacro %}" in logs[0]

    def test_basic_unmatched_endif(self):
        sql_content = """
        select *
        from my_table
        {% endif %}
        where x = 1
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endif %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endif %}" in logs[0]

    def test_matched_macro(self):
        sql_content = """
        {% macro my_macro() %}
        select *
        from my_table
        {% endmacro %}
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% macro my_macro() %}" in result
        assert "{% endmacro %}" in result
        assert len(logs) == 0

    def test_matched_if(self):
        sql_content = """
        {% if condition %}
        select *
        from my_table
        {% endif %}
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "if condition" in result
        assert "{% endif %}" in result
        assert len(logs) == 0

    def test_matched_if_with_parenthesis(self):
        sql_content = """
        {% if(condition) %}
        select *
        from my_table
        {% endif %}
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "if(condition)" in result
        assert "{% endif %}" in result
        assert len(logs) == 0

    def test_matched_if_with_macro_in_name(self):
        sql_content = """
        {% if(macro_test) %}
        select *
        from my_table
        {% endif %}
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "if(macro_test)" in result
        assert "{% endif %}" in result
        assert len(logs) == 0

    def test_nested_structures(self):
        sql_content = """
        {% macro outer_macro() %}
          {% if condition %}
            select *
            from my_table
          {% endif %}
        {% endmacro %}
        {% endif %}  -- This one is unmatched
        {% endmacro %}  -- This one is unmatched
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% macro outer_macro() %}" in result
        assert "{% if condition %}" in result
        assert len(logs) == 2
        assert any("Removed unmatched {% endif %}" in log for log in logs)
        assert any("Removed unmatched {% endmacro %}" in log for log in logs)

    def test_empty_and_no_tags(self):
        # Empty content
        result, logs = remove_unmatched_endings("")
        assert result == ""
        assert len(logs) == 0

        # No Jinja tags
        sql_content = """
        select *
        from my_table
        where x = 1
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert result.strip() == sql_content.strip()
        assert len(logs) == 0

    def test_multiline_tags(self):
        sql_content = """
        select *
        from my_table
        {% 
        endmacro
         %}
        where x = 1
        """
        result, logs = remove_unmatched_endings(sql_content)
        assert "endmacro" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endmacro %}" in logs[0]

    def test_whitespace_control(self):
        test_cases = [
            "{%- endmacro %}",  # Leading
            "{% endmacro -%}",  # Trailing
            "{%- endmacro -%}",  # Both
            """{%-
                endmacro
            -%}""",  # Multi-line
            """{%- if condition %}
            select 1
            {%- endif -%}
            {% endmacro %}""",  # Mixed
        ]

        for content in test_cases:
            result, logs = remove_unmatched_endings(content)
            assert "endmacro" not in result or (
                "{% if condition %}" in content and "{% endmacro %}" not in result
            )
            if "endmacro" in content and "if condition" not in content:
                assert len(logs) == 1
                assert "Removed unmatched {% endmacro %}" in logs[0]

    def test_line_numbers(self):
        # Single line
        sql_content = "{% macro test() %}select 1{% endmacro %}{% endif %}"
        result, logs = remove_unmatched_endings(sql_content)
        assert logs[0] == "Removed unmatched {% endif %} near line 1"

        # No leading newline
        sql_content = "{% macro test() %}\nselect 1\n{% endmacro %}\n{% endif %}"
        result, logs = remove_unmatched_endings(sql_content)
        assert logs[0] == "Removed unmatched {% endif %} near line 4"

        # With leading newline
        sql_content = "\n{% macro test() %}\nselect 1\n{% endmacro %}\n{% endif %}"
        result, logs = remove_unmatched_endings(sql_content)
        assert logs[0] == "Removed unmatched {% endif %} near line 5"

        # Mixed newlines
        sql_content = "{% macro test() %}\r\nselect 1\n{% endmacro %}\r\n{% endif %}"
        result, logs = remove_unmatched_endings(sql_content)
        assert logs[0] == "Removed unmatched {% endif %} near line 4"

    def test_in_comments(self):
        # Endif in comments
        sql_content = """-- This is a comment
        -- {% endif %}
        select * from table"""
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endif %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endif %}" in logs[0]

        # Endmacro in comments
        sql_content = """-- This is a comment
        select * from table
        -- {% endmacro %}"""
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endmacro %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endmacro %}" in logs[0]

    def test_after_other_tags(self):
        # After for loop
        sql_content = """{% for item in items %}
        select {{ item }} from table
        {% endfor %}
        {% endif %}"""
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endif %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endif %}" in logs[0]

        # After set statement
        sql_content = """{% set x = 5 %}
        select {{ x }} as value
        {% endmacro %}"""
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endmacro %}" not in result
        assert len(logs) == 1
        assert "Removed unmatched {% endmacro %}" in logs[0]

    def test_multiple_unmatched(self):
        sql_content = """select 1
        {% endif %}
        select 2
        {% endif %}
        select 3"""
        result, logs = remove_unmatched_endings(sql_content)
        assert "{% endif %}" not in result
        assert len(logs) == 2
        assert logs[0] == "Removed unmatched {% endif %} near line 2"
        assert logs[1] == "Removed unmatched {% endif %} near line 4"


class TestYamlRefactoring:
    """Tests for YAML refactoring functions"""

    def test_changeset_refactor_yml_with_duplicates(
        self, temp_project_dir, schema_yml_with_duplicates
    ):
        # Create a test YAML file
        yml_file = temp_project_dir / "models" / "schema.yml"
        yml_file.parent.mkdir(parents=True, exist_ok=True)
        yml_file.write_text(schema_yml_with_duplicates)

        # Test that loading a file with duplicates exits the program
        with pytest.raises(SystemExit):
            load_yaml_check_duplicates(yml_file)

    def test_changeset_refactor_yml_with_config_fields(
        self, temp_project_dir, schema_yml_with_config_fields
    ):
        # Create a test YAML file
        yml_file = temp_project_dir / "models" / "schema.yml"
        yml_file.parent.mkdir(parents=True, exist_ok=True)
        yml_file.write_text(schema_yml_with_config_fields)

        # Get the refactored result
        result = changeset_refactor_yml(yml_file)

        # Check that the file was refactored
        assert result.refactored
        assert isinstance(result, YMLRefactorResult)

        # Check that config fields were moved under config
        model = safe_load(result.refactored_yaml)["models"][0]

        assert "materialized" not in model
        assert "database" not in model
        assert "schema" not in model
        assert "config" in model
        assert model["config"]["materialized"] == "table"
        assert model["config"]["database"] == "my_db"
        assert model["config"]["schema"] == "my_schema"

        # Check that meta was merged correctly
        assert model["config"]["meta"]["abc"] == 123

    def test_changeset_all_yml_files(self, temp_project_dir, schema_yml_with_config_fields):
        # Create multiple YAML files
        models_dir = temp_project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        # Create a subdirectory with another YAML file
        sub_dir = models_dir / "example"
        sub_dir.mkdir(parents=True, exist_ok=True)
        sub_dir.joinpath("model.sql").write_text("not a YAML file")

        # Write YAML files
        models_dir.joinpath("schema.yml").write_text(schema_yml_with_config_fields)
        sub_dir.joinpath("other_schema.yaml").write_text(schema_yml_with_config_fields)

        # Get all refactored results
        results = changeset_all_sql_yml_files(temp_project_dir)

        # Check that we got results for both files
        assert len(results) == 2
        # Unpack the tuple of lists
        yaml_results, sql_results = results
        assert all(isinstance(r, YMLRefactorResult) for r in yaml_results)
        assert all(isinstance(r, SQLRefactorResult) for r in sql_results)
        assert all(r.refactored for r in yaml_results)

        # Check that both files were processed
        processed_files = {r.file_path for r in yaml_results}
        assert (models_dir / "schema.yml").resolve() in processed_files
        assert (sub_dir / "other_schema.yaml").resolve() in processed_files


class TestYamlOutput:
    """Tests for YAML output functions"""

    def test_output_yaml(self):
        # Test that output_yaml produces valid YAML
        test_data = {
            "version": 2,
            "models": [
                {
                    "name": "test_model",
                    "description": "Test model",
                    "config": {"materialized": "table", "meta": {"abc": 123}},
                }
            ],
        }

        yaml_str = output_yaml(test_data)
        assert "version: 2" in yaml_str
        assert "name: test_model" in yaml_str
        assert "materialized: table" in yaml_str
        assert "abc: 123" in yaml_str
