import pytest
import yaml
from pathlib import Path
from dbt_autofix.static_analysis_writer import (
    apply_strict_project_default,
    apply_baseline_to_model_schema,
    apply_baseline_to_model_sql,
    apply_static_analysis_config,
)

DBT_PROJECT = """\
name: my_project
version: '1.0.0'
models:
  my_project:
    +materialized: table
"""

SCHEMA_YML_WITH_MODEL = """\
version: 2
models:
  - name: customers
    description: "Customer model"
    columns:
      - name: customer_id
"""

SCHEMA_YML_WITH_CONFIG = """\
version: 2
models:
  - name: customers
    config:
      materialized: table
    columns:
      - name: customer_id
"""

SCHEMA_YML_TWO_MODELS = """\
version: 2
models:
  - name: orders
    description: "Orders"
  - name: customers
    description: "Customers"
"""


# --- apply_strict_project_default ---

def test_strict_added_at_project_level(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(DBT_PROJECT)
    apply_strict_project_default(tmp_path)
    data = yaml.safe_load((tmp_path / "dbt_project.yml").read_text())
    assert data["models"]["my_project"]["+static_analysis"] == "strict"


def test_strict_does_not_set_baseline(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(DBT_PROJECT)
    apply_strict_project_default(tmp_path)
    content = (tmp_path / "dbt_project.yml").read_text()
    assert "baseline" not in content


def test_strict_raises_when_no_dbt_project(tmp_path):
    with pytest.raises(FileNotFoundError):
        apply_strict_project_default(tmp_path)


# --- apply_baseline_to_model_schema ---

def test_baseline_added_to_model_without_config(tmp_path):
    schema_path = tmp_path / "models" / "schema.yml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(SCHEMA_YML_WITH_MODEL)
    apply_baseline_to_model_schema(tmp_path, "my_project://models/schema.yml", "customers")
    data = yaml.safe_load(schema_path.read_text())
    customers = next(m for m in data["models"] if m["name"] == "customers")
    assert customers["config"]["static_analysis"] == "baseline"


def test_baseline_adds_to_existing_config(tmp_path):
    schema_path = tmp_path / "models" / "schema.yml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(SCHEMA_YML_WITH_CONFIG)
    apply_baseline_to_model_schema(tmp_path, "my_project://models/schema.yml", "customers")
    data = yaml.safe_load(schema_path.read_text())
    customers = next(m for m in data["models"] if m["name"] == "customers")
    assert customers["config"]["static_analysis"] == "baseline"
    assert customers["config"]["materialized"] == "table"


def test_baseline_only_affects_target_model(tmp_path):
    schema_path = tmp_path / "models" / "schema.yml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(SCHEMA_YML_TWO_MODELS)
    apply_baseline_to_model_schema(tmp_path, "my_project://models/schema.yml", "customers")
    data = yaml.safe_load(schema_path.read_text())
    orders = next(m for m in data["models"] if m["name"] == "orders")
    assert orders.get("config", {}).get("static_analysis") != "baseline"


def test_baseline_schema_idempotent(tmp_path):
    schema_path = tmp_path / "models" / "schema.yml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(SCHEMA_YML_WITH_MODEL)
    apply_baseline_to_model_schema(tmp_path, "my_project://models/schema.yml", "customers")
    apply_baseline_to_model_schema(tmp_path, "my_project://models/schema.yml", "customers")
    data = yaml.safe_load(schema_path.read_text())
    customers = next(m for m in data["models"] if m["name"] == "customers")
    assert customers["config"]["static_analysis"] == "baseline"


def test_baseline_schema_skips_when_schema_missing(tmp_path):
    apply_baseline_to_model_schema(tmp_path, "my_project://models/nonexistent.yml", "customers")


# --- apply_baseline_to_model_sql ---

def test_baseline_injected_at_top_of_sql(tmp_path):
    sql_file = tmp_path / "models" / "customers.sql"
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("select * from raw_customers")
    apply_baseline_to_model_sql(tmp_path, "models/customers.sql")
    content = sql_file.read_text()
    assert content.startswith("{{ config(static_analysis='baseline') }}")
    assert "select * from raw_customers" in content


def test_baseline_not_double_injected(tmp_path):
    sql_file = tmp_path / "models" / "customers.sql"
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("{{ config(static_analysis='baseline') }}\nselect * from raw_customers")
    apply_baseline_to_model_sql(tmp_path, "models/customers.sql")
    assert sql_file.read_text().count("static_analysis") == 1


def test_baseline_sql_skips_when_file_missing(tmp_path):
    apply_baseline_to_model_sql(tmp_path, "models/nonexistent.sql")


# --- compatibility shim ---

def test_apply_static_analysis_config_still_works(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(DBT_PROJECT)
    apply_static_analysis_config(tmp_path, "baseline")
    data = yaml.safe_load((tmp_path / "dbt_project.yml").read_text())
    assert data["models"]["my_project"]["+static_analysis"] == "baseline"
