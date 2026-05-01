import pytest
from pathlib import Path
from dbt_autofix.static_analysis_writer import apply_static_analysis_config


BASE_DBT_PROJECT = """\
name: my_project
version: '1.0.0'
config-version: 2
models:
  my_project:
    +materialized: table
"""

DBT_PROJECT_WITH_EXISTING_STATIC_ANALYSIS = """\
name: my_project
version: '1.0.0'
models:
  my_project:
    +materialized: view
    +static_analysis: strict
"""

DBT_PROJECT_NO_MODELS_KEY = """\
name: my_project
version: '1.0.0'
config-version: 2
"""


def test_apply_adds_static_analysis_to_existing_models_config(tmp_path):
    f = tmp_path / "dbt_project.yml"
    f.write_text(BASE_DBT_PROJECT)
    apply_static_analysis_config(tmp_path, level="baseline")
    content = f.read_text()
    assert "+static_analysis: baseline" in content


def test_apply_updates_existing_static_analysis_key(tmp_path):
    f = tmp_path / "dbt_project.yml"
    f.write_text(DBT_PROJECT_WITH_EXISTING_STATIC_ANALYSIS)
    apply_static_analysis_config(tmp_path, level="baseline")
    import yaml
    data = yaml.safe_load(f.read_text())
    assert data["models"]["my_project"]["+static_analysis"] == "baseline"


def test_apply_creates_models_section_when_missing(tmp_path):
    f = tmp_path / "dbt_project.yml"
    f.write_text(DBT_PROJECT_NO_MODELS_KEY)
    apply_static_analysis_config(tmp_path, level="baseline")
    content = f.read_text()
    assert "models:" in content
    assert "+static_analysis: baseline" in content


def test_apply_raises_when_dbt_project_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        apply_static_analysis_config(tmp_path, level="baseline")


def test_apply_reads_project_name_for_nesting(tmp_path):
    f = tmp_path / "dbt_project.yml"
    f.write_text(BASE_DBT_PROJECT)
    apply_static_analysis_config(tmp_path, level="baseline")
    import yaml
    data = yaml.safe_load(f.read_text())
    assert "+static_analysis" in data["models"]["my_project"]
