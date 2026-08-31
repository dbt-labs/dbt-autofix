"""Update literal source() references after source-name normalization."""

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from dbt_autofix.refactors.results import SQLContent, SQLRefactorConfig, SQLRuleRefactorResult


@dataclass
class ResourceRenameMap:
    source_renames: dict[str, str] = field(default_factory=dict)


def build_resource_rename_map(path: Path) -> ResourceRenameMap:
    rename_map = ResourceRenameMap()
    for file_path in (*path.rglob("*.yml"), *path.rglob("*.yaml")):
        try:
            parsed = yaml.safe_load(file_path.read_text()) or {}
        except (OSError, yaml.YAMLError):
            continue
        for source in parsed.get("sources", []) or []:
            if isinstance(source, dict) and isinstance(source.get("name"), str):
                name = source["name"]
                normalized = _replace_spaces_outside_jinja(name)
                if normalized != name:
                    rename_map.source_renames[name] = normalized
    return rename_map


def _replace_spaces_outside_jinja(text: str) -> str:
    result = []
    in_jinja = False
    i = 0
    while i < len(text):
        if text[i : i + 2] == "{{":
            in_jinja = True
            result.append("{{")
            i += 2
        elif text[i : i + 2] == "}}":
            in_jinja = False
            result.append("}}")
            i += 2
        else:
            result.append("_" if text[i] == " " and not in_jinja else text[i])
            i += 1
    return "".join(result)


def update_resource_references_sql(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Rewrite the first literal argument of source() calls."""
    rename_map = getattr(config, "resource_rename_map", None)
    source_renames = getattr(rename_map, "source_renames", {}) if rename_map else {}
    pattern = re.compile(r"(\bsource\s*\(\s*)(?P<quote>['\"])(?P<name>.*?)(?P=quote)")

    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        new_name = source_renames.get(name, name)
        return f"{match.group(1)}{match.group('quote')}{new_name}{match.group('quote')}"

    refactored = pattern.sub(replace, content.current_str)
    changed = refactored != content.current_str
    return SQLRuleRefactorResult(
        rule_name="update_resource_references",
        refactored=changed,
        refactored_content=refactored,
        original_content=content.current_str,
        deprecation_refactors=[],
    )
