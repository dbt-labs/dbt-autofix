import json
from pathlib import Path
from dbt_autofix.static_analysis_report import (
    ModelAnalysisResult,
    ProjectAnalysisResult,
    StaticAnalysisLevel,
    analyze_project,
)
from dbt_autofix.sql_function_extractor import ModelFunctionScan


UNSUPPORTED = {"AGG", "OBJECT_CONSTRUCT_KEEP_NULL"}


def _scan(path_str: str, functions: set[str]) -> ModelFunctionScan:
    return ModelFunctionScan(model_path=Path(path_str), functions=functions)


def test_analyze_project_all_supported_recommends_strict():
    scans = [_scan("models/orders.sql", {"CONCAT", "TRIM"})]
    result = analyze_project(scans, UNSUPPORTED)
    assert result.recommended_level == StaticAnalysisLevel.STRICT
    assert result.models_with_issues == []


def test_analyze_project_with_unsupported_recommends_baseline():
    scans = [
        _scan("models/orders.sql", {"CONCAT", "AGG"}),
        _scan("models/customers.sql", {"TRIM"}),
    ]
    result = analyze_project(scans, UNSUPPORTED)
    assert result.recommended_level == StaticAnalysisLevel.BASELINE
    assert len(result.models_with_issues) == 1
    assert result.models_with_issues[0].unsupported_functions == {"AGG"}


def test_analyze_project_empty_scans_recommends_strict():
    result = analyze_project([], UNSUPPORTED)
    assert result.recommended_level == StaticAnalysisLevel.STRICT


def test_project_result_to_dict():
    scans = [_scan("models/orders.sql", {"AGG"})]
    result = analyze_project(scans, UNSUPPORTED)
    d = result.to_dict()
    assert d["recommended_level"] == "baseline"
    assert len(d["models_with_issues"]) == 1
    assert d["models_with_issues"][0]["unsupported_functions"] == ["AGG"]


def test_project_result_to_dict_is_json_serializable():
    scans = [_scan("models/orders.sql", {"AGG"})]
    result = analyze_project(scans, UNSUPPORTED)
    json.dumps(result.to_dict())  # must not raise
