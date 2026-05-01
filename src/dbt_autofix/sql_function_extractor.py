import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

SQL_KEYWORDS = frozenset({
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS",
    "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "USING",
    "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL",
    "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END", "WITH", "INSERT",
    "UPDATE", "DELETE", "CREATE", "TABLE", "VIEW", "OVER", "PARTITION",
    "ROWS", "RANGE", "BETWEEN", "UNBOUNDED", "PRECEDING", "FOLLOWING",
    "CURRENT", "ROW", "ASC", "DESC", "NULLS", "FIRST", "LAST", "FILTER",
    "WITHIN", "INTERVAL", "CAST", "TRY_CAST",
})

_FUNC_CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(")


@dataclass
class ModelFunctionScan:
    model_path: Path
    functions: set[str] = field(default_factory=set)


def extract_functions_from_sql(sql: str) -> set[str]:
    """Return uppercase SQL function names found in a SQL string, excluding keywords."""
    found = set()
    for match in _FUNC_CALL_RE.finditer(sql):
        name = match.group(1).upper()
        if name not in SQL_KEYWORDS:
            found.add(name)
    return found


def scan_compiled_dir(
    project_path: Path,
    select: Optional[list[str]] = None,
) -> list[ModelFunctionScan]:
    """Walk target/compiled/ and extract SQL function calls from every .sql file."""
    compiled_root = project_path / "target" / "compiled"
    if not compiled_root.exists():
        return []

    sql_files = list(compiled_root.rglob("*.sql"))

    if select:
        filtered = []
        for f in sql_files:
            rel = str(f.relative_to(compiled_root))
            if any(sel.rstrip("/") in rel for sel in select):
                filtered.append(f)
        sql_files = filtered

    results = []
    for sql_file in sql_files:
        try:
            content = sql_file.read_text(encoding="utf-8", errors="ignore")
            functions = extract_functions_from_sql(content)
            results.append(ModelFunctionScan(model_path=sql_file, functions=functions))
        except Exception:
            pass
    return results
