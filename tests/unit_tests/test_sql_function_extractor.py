import pytest
from pathlib import Path
from dbt_autofix.sql_function_extractor import (
    extract_functions_from_sql,
    scan_compiled_dir,
    SQL_KEYWORDS,
)


def test_extract_simple_function_calls():
    sql = "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users"
    result = extract_functions_from_sql(sql)
    assert "CONCAT" in result


def test_extract_is_case_insensitive():
    sql = "SELECT to_date(created_at) FROM orders"
    result = extract_functions_from_sql(sql)
    assert "TO_DATE" in result


def test_extract_excludes_sql_keywords():
    sql = "SELECT col FROM tbl WHERE col IS NOT NULL"
    result = extract_functions_from_sql(sql)
    for kw in SQL_KEYWORDS:
        assert kw not in result


def test_extract_multiple_functions():
    sql = "SELECT COALESCE(a, b), TRIM(c), AGG(d) FROM t"
    result = extract_functions_from_sql(sql)
    assert {"COALESCE", "TRIM", "AGG"}.issubset(result)


def test_extract_nested_functions():
    sql = "SELECT TO_DATE(TRIM(col), 'YYYY-MM-DD') FROM t"
    result = extract_functions_from_sql(sql)
    assert "TO_DATE" in result
    assert "TRIM" in result


def test_extract_empty_sql():
    assert extract_functions_from_sql("") == set()


def test_scan_compiled_dir(tmp_path):
    compiled = tmp_path / "target" / "compiled" / "my_project" / "models"
    compiled.mkdir(parents=True)
    (compiled / "orders.sql").write_text("SELECT CONCAT(a, b), AGG(c) FROM t")
    (compiled / "customers.sql").write_text("SELECT ANY_VALUE(name) FROM t")

    result = scan_compiled_dir(tmp_path)
    assert len(result) == 2
    paths = {str(r.model_path) for r in result}
    assert any("orders.sql" in p for p in paths)
    assert any("customers.sql" in p for p in paths)
    found_orders = next(r for r in result if "orders.sql" in str(r.model_path))
    assert "CONCAT" in found_orders.functions
    assert "AGG" in found_orders.functions


def test_scan_compiled_dir_missing_target_returns_empty(tmp_path):
    result = scan_compiled_dir(tmp_path)
    assert result == []


def test_scan_compiled_dir_respects_select(tmp_path):
    compiled = tmp_path / "target" / "compiled" / "my_project" / "models"
    compiled.mkdir(parents=True)
    subdir = compiled / "marts"
    subdir.mkdir()
    (compiled / "orders.sql").write_text("SELECT CONCAT(a, b) FROM t")
    (subdir / "revenue.sql").write_text("SELECT AGG(x) FROM t")

    result = scan_compiled_dir(tmp_path, select=["models/marts"])
    paths = [str(r.model_path) for r in result]
    assert all("marts" in p for p in paths)
    assert not any("orders.sql" in p for p in paths)
