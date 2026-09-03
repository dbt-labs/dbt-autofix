"""Driver for the deterministic dbt 1.x → 1.x migration (`dbt-migrate-1x`)."""

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, cast

from rich.console import Console
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactor import get_dbt_files_paths, skip_file
from dbt_autofix.refactors.results import (
    DbtProjectYMLRefactorConfig,
    SQLRefactorConfig,
    SQLRefactorResult,
    YMLRefactorConfig,
    YMLRefactorResult,
)
from dbt_autofix.refactors.yml import load_yaml
from dbt_autofix.retrieve_schemas import SchemaSpecs
from dbt_migrate_1x.changesets.dbt_1x import (
    changeset_1x_clean_targets,
    changeset_1x_rename_predicates_project,
    changeset_1x_rename_predicates_yml,
    changeset_1x_tests_to_data_tests_project,
    changeset_1x_tests_to_data_tests_yml,
    refactor_1x_current_timestamp,
    refactor_1x_rename_predicates_sql,
)

error_console = Console(stderr=True)

# The 1.x rules never read the Fusion JSON schema, so there is nothing to fetch from the CDN and
# nothing to pass here. The shared config dataclasses still require the field, so we hand them a
# typed None -- the same convention the existing test suite uses for schema-free changesets.
NO_SCHEMA_SPECS = cast(SchemaSpecs, None)

# Kinds map a rule to the file type it operates on.
KIND_PROJECT = "project"  # dbt_project.yml only
KIND_YAML = "yaml"  # schema YAML files (not dbt_project.yml)
KIND_SQL = "sql"  # .sql files


@dataclass
class Rule1x:
    """A deterministic 1.x rule tagged with the dbt version that introduced the change."""

    introduced: str  # "major.minor", e.g. "1.4"
    func: Callable
    kind: str


# The registry. Each rule fires when ``from_version < introduced <= to_version``.
RULES_1X: List[Rule1x] = [
    Rule1x("1.4", refactor_1x_current_timestamp, KIND_SQL),
    Rule1x("1.4", refactor_1x_rename_predicates_sql, KIND_SQL),
    Rule1x("1.4", changeset_1x_rename_predicates_yml, KIND_YAML),
    Rule1x("1.4", changeset_1x_rename_predicates_project, KIND_PROJECT),
    Rule1x("1.7", changeset_1x_clean_targets, KIND_PROJECT),
    Rule1x("1.8", changeset_1x_tests_to_data_tests_yml, KIND_YAML),
    Rule1x("1.8", changeset_1x_tests_to_data_tests_project, KIND_PROJECT),
]

MIN_VERSION = "1.3"
MAX_VERSION = "1.12"


def _version_tuple(version: str) -> Tuple[int, int]:
    parts = version.strip().split(".")
    try:
        return int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        raise ValueError(f"Invalid dbt version '{version}'. Expected 'major.minor', e.g. '1.8'.")


def _active_rules(from_version: str, to_version: str) -> List[Rule1x]:
    lo = _version_tuple(from_version)
    hi = _version_tuple(to_version)
    if lo >= hi:
        raise ValueError(f"--from ({from_version}) must be lower than --to ({to_version}).")
    return [rule for rule in RULES_1X if lo < _version_tuple(rule.introduced) <= hi]


def _process_project_yml(root_path: Path, rules: List[Callable], dry_run: bool) -> YMLRefactorResult:
    fp = root_path / "dbt_project.yml"
    yml_str = fp.read_text()
    try:
        original_parsed = load_yaml(yml_str)
    except Exception:
        original_parsed = CommentedMap()

    result = YMLRefactorResult(
        dry_run=dry_run,
        file_path=fp,
        original_parsed=original_parsed,
        refactored_yaml=yml_str,
        original_yaml=yml_str,
        refactors=[],
    )
    config = DbtProjectYMLRefactorConfig(NO_SCHEMA_SPECS, root_path)
    for func in rules:
        result.apply_changeset(func, config)
    return result


def _process_yaml_files(
    root_path: Path, model_paths: List[str], rules: List[Callable], dry_run: bool, select: Optional[List[str]]
) -> List[YMLRefactorResult]:
    results: List[YMLRefactorResult] = []
    seen: set[str] = set()
    config = YMLRefactorConfig(NO_SCHEMA_SPECS)

    for model_path in model_paths:
        base = (root_path / Path(model_path)).resolve()
        yaml_files = set(base.glob("**/*.yml")).union(base.glob("**/*.yaml"))
        for yml_file in yaml_files:
            if skip_file(yml_file, select) or str(yml_file) in seen:
                continue
            seen.add(str(yml_file))

            yml_str = yml_file.read_text()
            try:
                original_parsed = load_yaml(yml_str)
            except Exception:
                original_parsed = CommentedMap()
            result = YMLRefactorResult(
                dry_run=dry_run,
                file_path=yml_file,
                original_parsed=original_parsed,
                refactored_yaml=yml_str,
                original_yaml=yml_str,
                refactors=[],
            )
            for func in rules:
                result.apply_changeset(func, config)
            results.append(result)
    return results


def _process_sql_files(
    root_path: Path,
    sql_paths_to_node_type: Dict[str, str],
    rules: List[Callable],
    dry_run: bool,
    select: Optional[List[str]],
) -> List[SQLRefactorResult]:
    results: List[SQLRefactorResult] = []
    for sql_path, node_type in sql_paths_to_node_type.items():
        full_path = (root_path / sql_path).resolve()
        if not full_path.exists():
            continue
        config = SQLRefactorConfig(NO_SCHEMA_SPECS, node_type)
        for sql_file in full_path.glob("**/*.sql"):
            if skip_file(full_path, select):
                continue
            original_content = sql_file.read_text()
            result = SQLRefactorResult(
                dry_run=dry_run,
                file_path=sql_file,
                refactored_file_path=sql_file,
                refactored_content=original_content,
                original_content=original_content,
                refactors=[],
            )
            for func in rules:
                result.apply_changeset(func, config)
            results.append(result)
    return results


def migrate_1x_all_files(
    path: Path,
    dry_run: bool = False,
    select: Optional[List[str]] = None,
    from_version: str = MIN_VERSION,
    to_version: str = MAX_VERSION,
) -> Tuple[List[YMLRefactorResult], List[SQLRefactorResult]]:
    """Apply the deterministic 1.x rules active for the given version range.

    Returns a tuple of (yaml_results, sql_results). dbt_project.yml results are included in the
    yaml_results list. No Python-model rules exist in 1.x mode.
    """
    if not (path / "dbt_project.yml").exists():
        error_console.print(f"Error: dbt_project.yml not found in {path}", style="red")
        return [], []

    active = _active_rules(from_version, to_version)
    project_rules = [rule.func for rule in active if rule.kind == KIND_PROJECT]
    yaml_rules = [rule.func for rule in active if rule.kind == KIND_YAML]
    sql_rules = [rule.func for rule in active if rule.kind == KIND_SQL]

    # Process dbt_project.yml FIRST and write it before reading it back for paths, mirroring
    # changeset_all_files() so downstream path discovery sees the refactored file.
    project_results: List[YMLRefactorResult] = []
    if project_rules:
        result = _process_project_yml(path, project_rules, dry_run)
        project_results.append(result)
        if not dry_run and result.refactored:
            result.update_yaml_file()

    paths_to_node_type = get_dbt_files_paths(path)
    dbt_paths = list(paths_to_node_type.keys())

    sql_results = _process_sql_files(path, paths_to_node_type, sql_rules, dry_run, select) if sql_rules else []
    yaml_results = _process_yaml_files(path, dbt_paths, yaml_rules, dry_run, select) if yaml_rules else []

    return [*yaml_results, *project_results], sql_results
