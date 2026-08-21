import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List

from ruamel.yaml.comments import CommentedMap, CommentedSeq

from dbt_autofix.refactors.changesets.dbt_sql import extract_config_macro
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    DbtProjectYMLRefactorConfig,
    SQLContent,
    SQLRefactorConfig,
    SQLRuleRefactorResult,
    YMLContent,
    YMLRefactorConfig,
    YMLRuleRefactorResult,
)
from dbt_autofix.refactors.yml import DbtYAML, load_yaml


def _rename_key(mapping: CommentedMap, old_key: str, new_key: str) -> None:
    """Rename a key in a ruamel CommentedMap while preserving its position."""
    idx = list(mapping.keys()).index(old_key)
    value = mapping.pop(old_key)
    mapping.insert(idx, new_key, value)


def _walk_mappings(node: Any) -> Iterator[CommentedMap]:
    """Yield every mapping nested anywhere under `node`, outermost first."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk_mappings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_mappings(item)


def _config_mappings(node: Any) -> Iterator[CommentedMap]:
    """Yield every `config:` mapping nested anywhere under `node`."""
    for mapping in _walk_mappings(node):
        cfg = mapping.get("config")
        if isinstance(cfg, dict):
            yield cfg


def _rename_keys(
    mappings: Iterable[CommentedMap],
    renames: Dict[str, str],
    logs: List[str],
    log_fmt: str = "Renamed '{old}' to '{new}'",
) -> None:
    """Apply `renames` to every mapping in `mappings`, logging each rename.

    A key is only renamed when its replacement is absent, so an already-migrated node is never
    clobbered. `mappings` is materialized up front so renaming cannot disturb an in-progress walk.
    """
    for mapping in list(mappings):
        for old_key, new_key in renames.items():
            if old_key in mapping and new_key not in mapping:
                _rename_key(mapping, old_key, new_key)
                logs.append(log_fmt.format(old=old_key, new=new_key))


def _yml_result(
    rule_name: str, deprecation: str, yml_str: str, yml_dict: Any, logs: List[str]
) -> YMLRuleRefactorResult:
    refactored = len(logs) > 0
    return YMLRuleRefactorResult(
        rule_name=rule_name,
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=[DbtDeprecationRefactor(log=log, deprecation=deprecation) for log in logs],
    )


# ---------------------------------------------------------------------------
# 1.4 — current_timestamp macro moved from dbt-utils to dbt-core
# ---------------------------------------------------------------------------

# `\b` after current_timestamp prevents matching `current_timestamp_in_utc` (whose core
# equivalent is not a 1:1 rename, so we deliberately leave it alone).
CURRENT_TIMESTAMP_PATTERN = re.compile(r"dbt_utils\.current_timestamp\b")


def refactor_1x_current_timestamp(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Replace `dbt_utils.current_timestamp(...)` with `dbt.current_timestamp(...)`."""
    original = content.current_str
    new_content, n = CURRENT_TIMESTAMP_PATTERN.subn("dbt.current_timestamp", original)
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    if n > 0:
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Replaced {n} call(s) to 'dbt_utils.current_timestamp' with 'dbt.current_timestamp'",
                deprecation="CurrentTimestampMacroMigration",
            )
        )
    return SQLRuleRefactorResult(
        rule_name="migrate_1x_current_timestamp",
        refactored=n > 0,
        refactored_content=new_content,
        original_content=original,
        deprecation_refactors=deprecation_refactors,
    )


# ---------------------------------------------------------------------------
# 1.4 — incremental `predicates` config renamed to `incremental_predicates`
# ---------------------------------------------------------------------------

# Match a standalone `predicates=` kwarg; the negative lookbehind avoids matching the tail of
# an existing `incremental_predicates=`.
PREDICATES_KWARG_PATTERN = re.compile(r"(?<![\w])predicates(\s*=)")


def refactor_1x_rename_predicates_sql(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Rename the `predicates` kwarg to `incremental_predicates` inside a `{{ config(...) }}` block."""
    original = content.current_str
    macro = extract_config_macro(original)
    if not macro or not PREDICATES_KWARG_PATTERN.search(macro):
        return SQLRuleRefactorResult(
            rule_name="migrate_1x_rename_predicates_sql",
            refactored=False,
            refactored_content=original,
            original_content=original,
            deprecation_refactors=[],
        )

    new_macro = PREDICATES_KWARG_PATTERN.sub(r"incremental_predicates\1", macro)
    new_content = original.replace(macro, new_macro, 1)
    return SQLRuleRefactorResult(
        rule_name="migrate_1x_rename_predicates_sql",
        refactored=True,
        refactored_content=new_content,
        original_content=original,
        deprecation_refactors=[
            DbtDeprecationRefactor(
                log="Renamed config 'predicates' to 'incremental_predicates'",
                deprecation="IncrementalPredicatesRename",
            )
        ],
    )


_PREDICATES_RENAME = {"predicates": "incremental_predicates"}
# dbt_project.yml configs may carry the `+` prefix, which the rename must preserve.
_PREDICATES_RENAME_PROJECT = {**_PREDICATES_RENAME, "+predicates": "+incremental_predicates"}
_PREDICATES_LOG_FMT = "Renamed config '{old}' to '{new}'"


def changeset_1x_rename_predicates_yml(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Rename `predicates` -> `incremental_predicates` in `config:` blocks of schema YAML files."""
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    logs: List[str] = []
    _rename_keys(_config_mappings(yml_dict), _PREDICATES_RENAME, logs, _PREDICATES_LOG_FMT)
    return _yml_result("migrate_1x_rename_predicates_yml", "IncrementalPredicatesRename", yml_str, yml_dict, logs)


def changeset_1x_rename_predicates_project(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Rename `predicates` -> `incremental_predicates` under the `models:` config in dbt_project.yml."""
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    logs: List[str] = []
    _rename_keys(_walk_mappings(yml_dict.get("models")), _PREDICATES_RENAME_PROJECT, logs, _PREDICATES_LOG_FMT)
    return _yml_result("migrate_1x_rename_predicates_project", "IncrementalPredicatesRename", yml_str, yml_dict, logs)


# ---------------------------------------------------------------------------
# 1.7 — `dbt clean` errors when clean-targets include source paths or paths outside project
# ---------------------------------------------------------------------------

_PROJECT_PATH_KEY_DEFAULTS = {
    "model-paths": ["models"],
    "seed-paths": ["seeds"],
    "test-paths": ["tests"],
    "analysis-paths": ["analyses"],
    "snapshot-paths": ["snapshots"],
    "macro-paths": ["macros"],
}


def _resolved_paths(root: Path, value: Any) -> set[Path]:
    """Resolve a single path or list of paths against the project root."""
    if isinstance(value, str):
        raw: List[Any] = [value]
    elif isinstance(value, list):
        raw = value
    else:
        return set()
    # `root / v` yields v unchanged when v is already absolute, so this handles both forms.
    return {(root / str(v)).resolve() for v in raw if isinstance(v, str)}


def changeset_1x_clean_targets(content: YMLContent, config: DbtProjectYMLRefactorConfig) -> YMLRuleRefactorResult:
    """Drop clean-targets entries that match a configured source path or point outside the project."""
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    logs: List[str] = []

    clean_targets = yml_dict.get("clean-targets")
    if not isinstance(clean_targets, list):
        return _yml_result("migrate_1x_clean_targets", "CleanTargetsSourcePath", yml_str, yml_dict, logs)

    # dbt resolves every clean-target against the project dir before validating it
    # Compare fully resolved paths on both sides to match that behavior.
    root = config.root_path.resolve()

    # Collect every source path the project resolves resources from (explicit value or default).
    protected: set[Path] = set()
    for key, default in _PROJECT_PATH_KEY_DEFAULTS.items():
        protected |= _resolved_paths(root, yml_dict.get(key, default))
    # Legacy path keys still resolve to source directories in older projects.
    protected |= _resolved_paths(root, yml_dict.get("source-paths"))
    protected |= _resolved_paths(root, yml_dict.get("data-paths"))

    kept = CommentedSeq()
    for entry in clean_targets:
        resolved = (root / str(entry)).resolve()
        if resolved in protected:
            logs.append(f"Removed clean-target '{entry}' (it is a configured source path)")
        elif root not in resolved.parents:
            # dbt checks `project_dir not in path.absolute().parents`; `parents` excludes the
            # path itself, so a clean-target pointing at the project root also errors there.
            logs.append(f"Removed clean-target '{entry}' (it points outside the project)")
        else:
            kept.append(entry)

    if logs:
        yml_dict["clean-targets"] = kept

    return _yml_result("migrate_1x_clean_targets", "CleanTargetsSourcePath", yml_str, yml_dict, logs)


# ---------------------------------------------------------------------------
# 1.8 — `tests` config deprecated in favor of `data_tests`
# ---------------------------------------------------------------------------


_TESTS_RENAME = {"tests": "data_tests"}


def changeset_1x_tests_to_data_tests_yml(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Rename `tests:` -> `data_tests:` throughout a schema YAML file."""
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    logs: List[str] = []
    _rename_keys(_walk_mappings(yml_dict), _TESTS_RENAME, logs)
    return _yml_result("migrate_1x_tests_to_data_tests_yml", "TestsConfigDeprecation", yml_str, yml_dict, logs)


def changeset_1x_tests_to_data_tests_project(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Rename the top-level `tests:` config key to `data_tests:` in dbt_project.yml."""
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    logs: List[str] = []
    if "tests" in yml_dict and "data_tests" not in yml_dict:
        _rename_key(yml_dict, "tests", "data_tests")
        logs.append("Renamed top-level 'tests' config to 'data_tests'")
    return _yml_result("migrate_1x_tests_to_data_tests_project", "TestsConfigDeprecation", yml_str, yml_dict, logs)
