from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import sqlglot
import sqlglot.expressions as exp


@dataclass
class ModelFunctionScan:
    model_path: Path
    functions: set[str] = field(default_factory=set)


def extract_functions_from_sql(sql: str) -> set[str]:
    """Return uppercase SQL function names by walking the sqlglot AST.

    Uses Snowflake dialect so that dialect-specific constructs parse correctly.
    Anonymous functions (not mapped in sqlglot's Snowflake dialect) are included
    by their original name. Known functions are emitted using their Snowflake
    canonical name via the dialect generator.
    """
    if not sql.strip():
        return set()
    try:
        statements = sqlglot.parse(sql, dialect="snowflake", error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        return set()

    functions: set[str] = set()
    for statement in statements:
        if statement is None:
            continue
        for node in statement.walk():
            if isinstance(node, exp.Anonymous):
                # sqlglot couldn't map this to a known function — original name preserved
                functions.add(node.name.upper())
            elif isinstance(node, exp.Func):
                try:
                    # Generate back in Snowflake dialect to get the canonical function name
                    sql_repr = node.sql(dialect="snowflake")
                    name = sql_repr.split("(")[0].strip().upper()
                    if name and " " not in name and name.replace("_", "").isalnum():
                        functions.add(name)
                except Exception:
                    pass
    return functions


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
