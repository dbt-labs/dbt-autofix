from pathlib import Path

import pytest
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactors.changesets.dbt_sql import refactor_static_analysis_sql
from dbt_autofix.refactors.results import SQLContent, YMLContent
from dbt_autofix.refactors.static_analysis import (
    STATIC_ANALYSIS_DEPRECATION,
    changeset_normalize_static_analysis_yml,
    normalize_static_analysis_source,
    normalize_static_analysis_value,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, "baseline"),
        (False, "off"),
        ("true", "baseline"),
        ("True", "baseline"),
        ("yes", "baseline"),
        ("YES", "baseline"),
        ("false", "off"),
        ("no", "off"),
        # Already-valid enum values are left untouched.
        ("baseline", None),
        ("off", None),
        ("on", None),
        ("strict", None),
        ("unsafe", None),
        # Unrelated values are left untouched.
        (42, None),
        ("something_else", None),
    ],
)
def test_normalize_static_analysis_value(value, expected):
    assert normalize_static_analysis_value(value) == expected


@pytest.mark.parametrize(
    "source,expected",
    [
        ("True", "baseline"),
        ("False", "off"),
        ("true", "baseline"),
        ("false", "off"),
        ("'yes'", "baseline"),
        ('"no"', "off"),
        ("'baseline'", None),
        ("'off'", None),
        ("'on'", None),
        ("some_var()", None),
    ],
)
def test_normalize_static_analysis_source(source, expected):
    assert normalize_static_analysis_source(source) == expected


def _run_yml(yml_str: str) -> str:
    content = YMLContent(original_str=yml_str, original_parsed=CommentedMap(), current_str=yml_str)
    result = changeset_normalize_static_analysis_yml(content, None)
    return result.refactored_yaml if result.refactored else yml_str


def test_dbt_project_yml_conversion():
    yml_str = "\n".join(
        [
            "static_analysis: True",
            "models:",
            "  my_project:",
            "    +static_analysis: False",
            "    staging:",
            "      +static_analysis: yes",
            "    valid_zone:",
            "      +static_analysis: baseline",
            "    off_zone:",
            "      +static_analysis: off",
        ]
    )
    out = _run_yml(yml_str)
    assert "static_analysis: 'baseline'" in out
    assert "+static_analysis: 'off'" in out
    # Already-valid enum values are preserved as-is (unquoted).
    assert "+static_analysis: baseline" in out
    assert "+static_analysis: off" in out


def test_schema_yml_conversion():
    yml_str = "\n".join(
        [
            "models:",
            "  - name: my_model",
            "    config:",
            "      static_analysis: True",
            "  - name: valid_model",
            "    config:",
            "      static_analysis: strict",
        ]
    )
    out = _run_yml(yml_str)
    assert "static_analysis: 'baseline'" in out
    assert "static_analysis: strict" in out


def test_schema_yml_no_change_is_noop():
    yml_str = "\n".join(
        [
            "models:",
            "  - name: valid_model",
            "    config:",
            "      static_analysis: baseline",
        ]
    )
    content = YMLContent(original_str=yml_str, original_parsed=CommentedMap(), current_str=yml_str)
    result = changeset_normalize_static_analysis_yml(content, None)
    assert result.refactored is False


def _run_sql(sql: str) -> str:
    content = SQLContent(original_str=sql, current_str=sql, current_file_path=Path("x.sql"))
    result = refactor_static_analysis_sql(content, None)
    return result.refactored_content


def test_sql_config_conversion_true():
    sql = "{{ config(\n    materialized='table',\n    static_analysis=True\n) }}\n\nselect 1 as id\n"
    out = _run_sql(sql)
    assert "static_analysis='baseline'" in out
    assert "materialized='table'" in out


def test_sql_config_conversion_false():
    sql = "{{ config(static_analysis=False) }}\nselect 1"
    out = _run_sql(sql)
    assert "static_analysis='off'" in out


def test_sql_config_ignores_config_in_comment():
    # A ``{{ config() }}`` in a comment must not shadow the real config block.
    sql = (
        "-- example {{ config() }} in a comment\n"
        "-- static_analysis=True should be rewritten\n"
        "{{ config(\n    materialized='view',\n    static_analysis=True\n) }}\n\nselect 1\n"
    )
    out = _run_sql(sql)
    assert "static_analysis='baseline'" in out


def test_sql_config_already_valid_is_noop():
    sql = "{{ config(static_analysis='strict') }}\nselect 1"
    content = SQLContent(original_str=sql, current_str=sql, current_file_path=Path("x.sql"))
    result = refactor_static_analysis_sql(content, None)
    assert result.refactored is False


def test_sql_config_dict_repeated_key_keeps_last_value():
    """Normalizing static_analysis collapses the dict, so a repeated key resolves as dbt does."""
    sql = '{{ config({"materialized": "table", "tags": "first", "tags": "second", "static_analysis": True}) }}\nselect 1 as id\n'
    content = SQLContent(original_str=sql, current_str=sql, current_file_path=Path("x.sql"))
    result = refactor_static_analysis_sql(content, None)

    assert result.refactored
    assert 'tags="second"' in result.refactored_content
    assert result.refactor_logs == [
        "Config key 'tags' was defined 2 times in the config() dictionary; "
        'kept the last value ("second") and dropped the earlier one ("first"), '
        "which is how dbt resolves a repeated key.",
        "Converted static_analysis=True to static_analysis='baseline' (static_analysis must be a Fusion enum value)",
    ]
    # The duplicate note carries no deprecation; the static_analysis conversion does
    assert result.deprecation_refactors[0].deprecation is None
    assert result.deprecation_refactors[1].deprecation == STATIC_ANALYSIS_DEPRECATION
