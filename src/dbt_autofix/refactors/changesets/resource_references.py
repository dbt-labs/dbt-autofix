"""Rewrite ref()/source() references to resources renamed by the "spaces in resource names" fix.

When the "spaces in resource names" behavior-change fixes rename a resource (by renaming its
YAML ``name:`` and/or its ``.sql``/``.py`` file, space -> underscore), any existing
``ref('model with spaces')`` / ``source('src with spaces', ...)`` references are left dangling
and break the project. This module:

1. Builds a :class:`ResourceRenameMap` (old name -> new name) from the resources being renamed,
   using the *same* normalization the rename uses (see ``_replace_spaces_outside_jinja``).
2. Provides changeset functions to rewrite the references in SQL/Jinja, Python, and YAML.

Only references pointing at resources actually being renamed by this run are rewritten.
"""

import re
from pathlib import Path
from typing import Dict, List

import yaml as pyyaml

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.changesets.dbt_schema_yml import _replace_spaces_outside_jinja
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    PythonContent,
    PythonRefactorConfig,
    PythonRuleRefactorResult,
    ResourceRenameMap,
    SQLContent,
    SQLRefactorConfig,
    SQLRuleRefactorResult,
    YMLContent,
    YMLRefactorConfig,
    YMLRuleRefactorResult,
)

# Node types whose renamed `name:` values are targets of ref()
_REF_NODE_TYPES = ("models", "seeds", "snapshots")


def _normalized_rename(name: str) -> str:
    """Apply the same normalization the rename fixes use (space -> underscore, Jinja-safe)."""
    return _replace_spaces_outside_jinja(name)


def build_resource_rename_map(
    root_path: Path,
    dbt_paths_to_node_type: Dict[str, str],
) -> ResourceRenameMap:
    """Compute the old-name -> new-name map for resources renamed by the space-in-names fixes.

    Sources of renames (unioned):
    - YAML ``name:`` values for models/seeds/snapshots (targets of ref) and sources
      (first arg of source) that contain a space.
    - ``.sql`` / ``.py`` model/seed/snapshot files whose *stem* contains a space (these are
      referenceable by their file name even without a YAML ``name:`` entry).
    """
    rename_map = ResourceRenameMap()

    # 1) YAML name renames
    for path_str, node_type in dbt_paths_to_node_type.items():
        full_path = (root_path / path_str).resolve()
        if not full_path.exists():
            continue
        for yml_file in list(full_path.glob("**/*.yml")) + list(full_path.glob("**/*.yaml")):
            try:
                parsed = pyyaml.safe_load(yml_file.read_text())
            except Exception:
                continue
            if not isinstance(parsed, dict):
                continue

            for yaml_node_type in _REF_NODE_TYPES:
                for node in parsed.get(yaml_node_type, []) or []:
                    if isinstance(node, dict):
                        _record_node_rename(rename_map.node_renames, node.get("name"))

            for source in parsed.get("sources", []) or []:
                if isinstance(source, dict):
                    _record_node_rename(rename_map.source_renames, source.get("name"))

    # 2) File-name renames (model/seed/snapshot files referenceable by file stem)
    for path_str, node_type in dbt_paths_to_node_type.items():
        if node_type not in _REF_NODE_TYPES:
            continue
        full_path = (root_path / path_str).resolve()
        if not full_path.exists():
            continue
        for ext in ("*.sql", "*.py"):
            for model_file in full_path.glob(f"**/{ext}"):
                _record_node_rename(rename_map.node_renames, model_file.stem)

    return rename_map


def _record_node_rename(target: Dict[str, str], name) -> None:
    if not isinstance(name, str) or not name:
        return
    new_name = _normalized_rename(name)
    if new_name != name:
        target[name] = new_name


def _build_call_pattern(func_name: str) -> re.Pattern:
    """Regex matching ref(...) / source(...) calls, capturing the argument list."""
    # dbt allows an optional namespace, e.g. `dbt.ref(...)` in Python; the leading part
    # is matched but not captured so we can rebuild it verbatim.
    return re.compile(
        r"(?P<prefix>\b(?:dbt\s*\.\s*)?" + func_name + r"\s*\(\s*)"
        r"(?P<args>[^)]*?)"
        r"(?P<suffix>\s*\))",
        re.DOTALL,
    )


_REF_PATTERN = _build_call_pattern("ref")
_SOURCE_PATTERN = _build_call_pattern("source")

# Matches a single quoted string literal, capturing quote char and inner value.
_QUOTED_STRING = re.compile(r"(?P<q>['\"])(?P<val>(?:\\.|[^\\])*?)(?P=q)")


def _rewrite_refs(content: str, node_renames: Dict[str, str]) -> tuple[str, int]:
    """Rewrite the model-name argument of ref() calls per the rename map.

    Handles single-arg ``ref('name')`` and two-arg ``ref('pkg', 'name')`` — only the
    trailing model-name positional argument is considered a rename target.
    """
    if not node_renames:
        return content, 0

    count = 0

    def _replace(match: re.Match) -> str:
        nonlocal count
        args = match.group("args")
        # Find positional (non-keyword) quoted string literals in order.
        literals = list(_QUOTED_STRING.finditer(args))
        if not literals:
            return match.group(0)
        # The model name is the last positional quoted literal that is not a kwarg value
        # (version=... uses a bare number, so quoted literals here are pkg/name positionals).
        name_literal = literals[-1]
        old_name = name_literal.group("val")
        if old_name not in node_renames:
            return match.group(0)
        new_name = node_renames[old_name]
        new_args = args[: name_literal.start("val")] + new_name + args[name_literal.end("val") :]
        count += 1
        return match.group("prefix") + new_args + match.group("suffix")

    new_content = _REF_PATTERN.sub(_replace, content)
    return new_content, count


def _rewrite_sources(content: str, source_renames: Dict[str, str]) -> tuple[str, int]:
    """Rewrite the source-name (first) argument of source() calls per the rename map."""
    if not source_renames:
        return content, 0

    count = 0

    def _replace(match: re.Match) -> str:
        nonlocal count
        args = match.group("args")
        literals = list(_QUOTED_STRING.finditer(args))
        if not literals:
            return match.group(0)
        source_literal = literals[0]  # first positional arg is the source name
        old_name = source_literal.group("val")
        if old_name not in source_renames:
            return match.group(0)
        new_name = source_renames[old_name]
        new_args = args[: source_literal.start("val")] + new_name + args[source_literal.end("val") :]
        count += 1
        return match.group("prefix") + new_args + match.group("suffix")

    new_content = _SOURCE_PATTERN.sub(_replace, content)
    return new_content, count


def _rewrite_text(content: str, rename_map: ResourceRenameMap) -> tuple[str, List[DbtDeprecationRefactor]]:
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    content, ref_count = _rewrite_refs(content, rename_map.node_renames)
    content, source_count = _rewrite_sources(content, rename_map.source_renames)
    if ref_count:
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Updated {ref_count} ref() reference(s) to renamed resource(s).",
                deprecation=DeprecationType.RESOURCE_NAMES_WITH_SPACES_DEPRECATION,
            )
        )
    if source_count:
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Updated {source_count} source() reference(s) to renamed source(s).",
                deprecation=DeprecationType.RESOURCE_NAMES_WITH_SPACES_DEPRECATION,
            )
        )
    return content, deprecation_refactors


def update_resource_references_sql(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Rewrite ref()/source() references in SQL/Jinja files to renamed resources."""
    sql_content = content.current_str
    rename_map = config.resource_rename_map
    if rename_map is None or rename_map.is_empty():
        return SQLRuleRefactorResult(
            rule_name="update_resource_references",
            refactored=False,
            refactored_content=sql_content,
            original_content=sql_content,
            deprecation_refactors=[],
        )

    new_content, deprecation_refactors = _rewrite_text(sql_content, rename_map)
    refactored = bool(deprecation_refactors)
    return SQLRuleRefactorResult(
        rule_name="update_resource_references",
        refactored=refactored,
        refactored_content=new_content if refactored else sql_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
    )


def update_resource_references_python(content: PythonContent, config: PythonRefactorConfig) -> PythonRuleRefactorResult:
    """Rewrite dbt.ref()/dbt.source() references in Python models to renamed resources."""
    python_content = content.current_str
    rename_map = config.resource_rename_map
    if rename_map is None or rename_map.is_empty():
        return PythonRuleRefactorResult(
            rule_name="update_resource_references",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
        )

    new_content, deprecation_refactors = _rewrite_text(python_content, rename_map)
    refactored = bool(deprecation_refactors)
    return PythonRuleRefactorResult(
        rule_name="update_resource_references",
        refactored=refactored,
        refactored_content=new_content if refactored else python_content,
        original_content=python_content,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_update_resource_references_yml(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Rewrite ref()/source() references embedded in YAML files (e.g. exposures ``depends_on``).

    References in YAML are expressed as Jinja strings such as ``"ref('model with spaces')"``.
    We rewrite them the same way as SQL/Jinja content, operating on the raw YAML text so that
    formatting and comments are preserved.
    """
    yml_str = content.current_str
    rename_map = config.resource_rename_map
    if rename_map is None or rename_map.is_empty() or ("ref(" not in yml_str and "source(" not in yml_str):
        return YMLRuleRefactorResult(
            rule_name="update_resource_references",
            refactored=False,
            refactored_yaml=yml_str,
            original_yaml=yml_str,
            deprecation_refactors=[],
        )

    new_yaml, deprecation_refactors = _rewrite_text(yml_str, rename_map)
    refactored = bool(deprecation_refactors)
    return YMLRuleRefactorResult(
        rule_name="update_resource_references",
        refactored=refactored,
        refactored_yaml=new_yaml if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )
