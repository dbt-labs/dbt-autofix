import json
import shutil
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from dbt_autofix.main import app

runner = CliRunner()

FIXTURE_PROJECT = Path(__file__).parent / "fusion_static_analysis_fixtures" / "project_fusion_static_analysis"


@pytest.fixture()
def project(tmp_path):
    """Copy the fixture project to a temp dir so tests can mutate it."""
    dest = tmp_path / "project_fusion_static_analysis"
    shutil.copytree(FIXTURE_PROJECT, dest)
    return dest


# ---------------------------------------------------------------------------
# Report-only mode (no --apply)
# ---------------------------------------------------------------------------


def test_clean_project_exits_0(project):
    """A project using only supported functions should exit 0 with no issues."""
    # Remove dirty_model so only clean_model remains
    (project / "target" / "compiled" / "my_project" / "models" / "dirty_model.sql").unlink()

    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project)])
    assert result.exit_code == 0
    assert "strict" in result.output


def test_dirty_project_exits_1(project):
    """A project with unsupported functions should exit 1."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project)])
    assert result.exit_code == 1


def test_dirty_project_reports_unsupported_functions(project):
    """Output should name the unsupported functions found."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project)])
    assert "AGG" in result.output
    assert "PERCENTILE_CONT" in result.output


def test_dirty_project_names_affected_model(project):
    """Output should identify which model file contains the unsupported functions."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--json"])
    lines = [l for l in result.output.strip().splitlines() if l.strip()]
    report = json.loads(lines[0])
    assert any("dirty_model.sql" in issue["model_path"] for issue in report["models_with_issues"])


def test_clean_model_not_flagged(project):
    """clean_model.sql uses only supported functions and should not appear in output."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project)])
    assert "clean_model.sql" not in result.output


# ---------------------------------------------------------------------------
# --apply flag
# ---------------------------------------------------------------------------


def test_apply_writes_strict_to_dbt_project_yml(project):
    """--apply should set +static_analysis: strict in dbt_project.yml as the project default."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--apply"])
    assert result.exit_code == 1  # still exits 1 because issues were found

    dbt_project_content = (project / "dbt_project.yml").read_text()
    assert "+static_analysis: strict" in dbt_project_content
    assert "+static_analysis: baseline" not in dbt_project_content


def test_apply_writes_baseline_to_offending_model_schema(project):
    """--apply should write static_analysis: baseline into the offending model's schema YAML."""
    runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--apply"])

    schema_data = yaml.safe_load((project / "models" / "schema.yml").read_text())
    dirty = next(m for m in schema_data["models"] if m["name"] == "dirty_model")
    assert dirty["config"]["static_analysis"] == "baseline"


def test_apply_does_not_touch_clean_model_schema(project):
    """--apply should NOT add baseline config to models that only use supported functions."""
    runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--apply"])

    schema_data = yaml.safe_load((project / "models" / "schema.yml").read_text())
    clean = next(m for m in schema_data["models"] if m["name"] == "clean_model")
    assert clean.get("config", {}).get("static_analysis") != "baseline"


def test_apply_does_not_modify_clean_project(project):
    """--apply on a clean project should not touch dbt_project.yml."""
    (project / "target" / "compiled" / "my_project" / "models" / "dirty_model.sql").unlink()
    original = (project / "dbt_project.yml").read_text()

    runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--apply"])

    assert (project / "dbt_project.yml").read_text() == original


def test_dry_run_does_not_write_config(project):
    """--apply --dry-run should report but not write anything."""
    original_project = (project / "dbt_project.yml").read_text()
    original_schema = (project / "models" / "schema.yml").read_text()
    runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--apply", "--dry-run"])
    assert (project / "dbt_project.yml").read_text() == original_project
    assert (project / "models" / "schema.yml").read_text() == original_schema


# ---------------------------------------------------------------------------
# --json flag
# ---------------------------------------------------------------------------


def test_json_output_is_valid(project):
    """--json output should be valid JSON lines ending with {"mode": "complete"}."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--json"])
    lines = [l for l in result.output.strip().splitlines() if l.strip()]
    parsed = [json.loads(line) for line in lines]  # must not raise

    last = parsed[-1]
    assert last == {"mode": "complete"}


def test_json_output_contains_findings(project):
    """JSON output should include recommended_level and models_with_issues."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(project), "--json"])
    lines = [l for l in result.output.strip().splitlines() if l.strip()]
    report = json.loads(lines[0])

    assert report["recommended_level"] == "baseline"
    assert len(report["models_with_issues"]) == 1
    issue = report["models_with_issues"][0]
    assert "dirty_model.sql" in issue["model_path"]
    assert "AGG" in issue["unsupported_functions"]


# ---------------------------------------------------------------------------
# --select flag
# ---------------------------------------------------------------------------


def test_select_limits_scope(project):
    """--select pointing only at the clean subfolder should find no issues."""
    # Move clean_model into a subfolder and scope to just that folder
    clean_dir = project / "target" / "compiled" / "my_project" / "models" / "clean"
    clean_dir.mkdir()
    (project / "target" / "compiled" / "my_project" / "models" / "clean_model.sql").rename(
        clean_dir / "clean_model.sql"
    )

    result = runner.invoke(
        app,
        ["fusion-static-analysis", "--path", str(project), "--select", "models/clean"],
    )
    assert result.exit_code == 0
    assert "dirty_model.sql" not in result.output


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_missing_compiled_dir_exits_1(tmp_path):
    """A project with no target/compiled/ should exit 1 with a helpful message."""
    (tmp_path / "dbt_project.yml").write_text("name: empty_project\n")
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(tmp_path)])
    assert result.exit_code == 1
    assert "target/compiled" in result.output


def test_nonexistent_path_exits_1(tmp_path):
    """Passing a nonexistent --path should exit 1."""
    result = runner.invoke(app, ["fusion-static-analysis", "--path", str(tmp_path / "no_such_dir")])
    assert result.exit_code == 1
