import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from rich.console import Console
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactors.fancy_quotes_utils import restore_fancy_quotes
from dbt_autofix.refactors.yml import load_yaml
from dbt_autofix.semantic_definitions import SemanticDefinitions

console = Console()


@dataclass
class Location:
    line: int
    start: Optional[int] = None
    end: Optional[int] = None

    def to_dict(self) -> dict:
        d: dict = {"line": self.line}
        if self.start is not None:
            d["start"] = self.start
        if self.end is not None:
            d["end"] = self.end
        return d


@dataclass
class DbtDeprecationRefactor:
    log: str
    deprecation: Optional[str] = None
    original_location: Optional[Location] = None
    edited_location: Optional[Location] = None

    def to_dict(self) -> dict:
        ret_dict: dict = {"deprecation": self.deprecation, "log": self.log}
        if self.original_location is not None:
            ret_dict["original_location"] = self.original_location.to_dict()
        if self.edited_location is not None:
            ret_dict["edited_location"] = self.edited_location.to_dict()
        return ret_dict


# ---------------------------------------------------------------------------
# Content dataclasses (constructed by apply_changeset, passed to changeset functions)
# ---------------------------------------------------------------------------


@dataclass
class YMLContent:
    original_str: str
    original_parsed: Any  # CommentedMap from ruamel, parsed once at YMLRefactorResult creation
    current_str: str


@dataclass
class SQLContent:
    original_str: str
    current_str: str
    current_file_path: Path


@dataclass
class PythonContent:
    original_str: str
    current_str: str


# ---------------------------------------------------------------------------
# Config dataclasses (built once per file batch, passed to each changeset)
# ---------------------------------------------------------------------------


@dataclass
class YMLRefactorConfig:
    schema_specs: Any  # SchemaSpecs
    semantic_definitions: Optional[SemanticDefinitions] = None


@dataclass
class DbtProjectYMLRefactorConfig:
    schema_specs: Any  # SchemaSpecs
    root_path: Path
    exclude_dbt_project_keys: bool = False


@dataclass
class SQLRefactorConfig:
    schema_specs: Any  # SchemaSpecs
    node_type: str


@dataclass
class PythonRefactorConfig:
    schema_specs: Any  # SchemaSpecs
    node_type: str


# ---------------------------------------------------------------------------
# Location utility for YAML key position tracking
# ---------------------------------------------------------------------------


def find_key_line(yml_str: str, key: str) -> Optional[Location]:
    """Find the line and column of a key in a YAML string.

    Supports both top-level and indented keys. Returns the first match.
    """
    m = re.search(rf"^(\s*)({re.escape(key)}\s*:)", yml_str, re.MULTILINE)
    if m:
        prefix = yml_str[: m.start()]
        line = prefix.count("\n") + 1
        return Location(line=line, start=m.start(2) - m.start(), end=m.end(2) - m.start())
    return None


def find_key_at_path(node: CommentedMap, path: list) -> Optional[Location]:
    """Find the Location of a key by navigating a path through a parsed ruamel.yaml object.

    path segments are str (dict key) or int (list index).
    The last segment must be a str key whose location is returned.
    """
    if not path:
        return None
    current = node
    for segment in path[:-1]:
        if current is None:
            return None
        try:
            current = current[segment]
        except (KeyError, IndexError, TypeError):
            return None
    return location_of_key(current, path[-1])


def location_of_key(node: CommentedMap, key: str) -> Optional[Location]:
    """Return the Location of a key directly in a CommentedMap node."""
    try:
        line, col = node.lc.key(key)
        return Location(line=line + 1, start=col, end=col + len(key) + 1)
    except (AttributeError, KeyError, IndexError):
        return None


def location_of_node(node: CommentedMap) -> Optional[Location]:
    """Return the Location of a CommentedMap node (e.g., a list item)."""
    try:
        return Location(line=node.lc.line + 1)
    except (AttributeError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Rule-level result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class YMLRuleRefactorResult:
    rule_name: str
    refactored: bool
    refactored_yaml: str
    original_yaml: str
    deprecation_refactors: list[DbtDeprecationRefactor]
    pending_location_resolution: list[Callable[[CommentedMap], None]] = field(
        default_factory=list, repr=False, compare=False
    )

    @property
    def refactor_logs(self):
        return [refactor.log for refactor in self.deprecation_refactors]

    def to_dict(self) -> dict:
        ret_dict = {
            "deprecation_refactors": [
                deprecation_refactor.to_dict() for deprecation_refactor in self.deprecation_refactors
            ]
        }
        return ret_dict


@dataclass
class SQLRuleRefactorResult:
    rule_name: str
    refactored: bool
    refactored_content: str
    original_content: str
    deprecation_refactors: list[DbtDeprecationRefactor]
    refactored_file_path: Optional[Path] = None
    refactor_warnings: list[str] = field(default_factory=list)

    @property
    def refactor_logs(self):
        return [refactor.log for refactor in self.deprecation_refactors]

    def to_dict(self) -> dict:
        ret_dict = {
            "rule_name": self.rule_name,
            "deprecation_refactors": [refactor.to_dict() for refactor in self.deprecation_refactors],
        }
        return ret_dict


@dataclass
class PythonRuleRefactorResult:
    rule_name: str
    refactored: bool
    refactored_content: str
    original_content: str
    deprecation_refactors: list[DbtDeprecationRefactor]
    refactor_warnings: list[str] = field(default_factory=list)

    @property
    def refactor_logs(self):
        return [refactor.log for refactor in self.deprecation_refactors]

    def to_dict(self) -> dict:
        ret_dict = {
            "rule_name": self.rule_name,
            "deprecation_refactors": [refactor.to_dict() for refactor in self.deprecation_refactors],
        }
        return ret_dict


# ---------------------------------------------------------------------------
# File-level result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class YMLRefactorResult:
    dry_run: bool
    file_path: Path
    original_parsed: Any
    refactored_yaml: str
    original_yaml: str
    refactors: list[YMLRuleRefactorResult]

    @property
    def refactored(self) -> bool:
        return any(r.refactored for r in self.refactors)

    def apply_changeset(self, func: Callable, config: Any) -> None:
        content = YMLContent(
            original_str=self.original_yaml,
            original_parsed=self.original_parsed,
            current_str=self.refactored_yaml,
        )
        result = func(content, config)
        if result.refactored:
            self.refactors.append(result)
            self.refactored_yaml = result.refactored_yaml

    def resolve_pending_locations(self) -> None:
        pending = [resolve for refactor in self.refactors for resolve in refactor.pending_location_resolution]
        if pending:
            final_parsed = load_yaml(self.refactored_yaml)
            for resolve in pending:
                resolve(final_parsed)

    def update_yaml_file(self) -> None:
        """Update the YAML file with the refactored content"""
        # Restore fancy quotes from placeholders before writing
        final_yaml = restore_fancy_quotes(self.refactored_yaml)
        Path(self.file_path).write_text(final_yaml)

    def print_to_console(self, json_output: bool = True):
        if not self.refactored:
            return

        if json_output:
            flattened_refactors = []
            for refactor in self.refactors:
                if refactor.refactored:
                    flattened_refactors.extend(refactor.to_dict()["deprecation_refactors"])

            to_print = {
                "mode": "dry_run" if self.dry_run else "applied",
                "file_path": str(self.file_path),
                "refactors": flattened_refactors,
            }
            print(json.dumps(to_print))
            return

        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for refactor in self.refactors:
            if refactor.refactored:
                console.print(f"  {refactor.rule_name}", style="yellow")

                for dr in refactor.deprecation_refactors:
                    loc_suffix = f" (line {dr.original_location.line})" if dr.original_location else ""
                    console.print(f"    {dr.log}{loc_suffix}")


@dataclass
class SQLRefactorResult:
    dry_run: bool
    file_path: Path
    refactored_file_path: Path
    refactored_content: str
    original_content: str
    refactors: list[SQLRuleRefactorResult]
    has_warnings: bool = False

    @property
    def refactored(self) -> bool:
        return any(r.refactored for r in self.refactors) or (self.refactored_file_path != self.file_path)

    def apply_changeset(self, func: Callable, config: Any) -> None:
        content = SQLContent(
            original_str=self.original_content,
            current_str=self.refactored_content,
            current_file_path=self.refactored_file_path,
        )
        result = func(content, config)
        self.refactors.append(result)
        if result.refactored:
            self.refactored_content = result.refactored_content
            if result.refactored_file_path:
                self.refactored_file_path = result.refactored_file_path
        if result.refactor_warnings:
            self.has_warnings = True

    def update_sql_file(self) -> None:
        """Update the SQL file with the refactored content"""
        new_file_path = self.refactored_file_path or self.file_path
        if self.file_path != new_file_path:
            os.rename(self.file_path, self.refactored_file_path)

        Path(new_file_path).write_text(self.refactored_content)

    def print_to_console(self, json_output: bool = True):
        if not self.refactored and not self.has_warnings:
            return

        if json_output:
            flattened_refactors = []
            for refactor in self.refactors:
                if refactor.refactored:
                    flattened_refactors.extend(refactor.to_dict()["deprecation_refactors"])

            flattened_warnings = []
            for refactor in self.refactors:
                if refactor.refactor_warnings:
                    flattened_warnings.extend(refactor.refactor_warnings)

            to_print = {
                "mode": "dry_run" if self.dry_run else "applied",
                "file_path": str(self.file_path),
                "refactors": flattened_refactors,
                "warnings": flattened_warnings,
            }
            print(json.dumps(to_print))
            return

        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for refactor in self.refactors:
            if refactor.refactored:
                console.print(f"  {refactor.rule_name}", style="yellow")

                for dr in refactor.deprecation_refactors:
                    loc_suffix = f" (line {dr.original_location.line})" if dr.original_location else ""
                    console.print(f"    {dr.log}{loc_suffix}")

                for warning in refactor.refactor_warnings:
                    console.print(f"    Warning: {warning}", style="red")
            elif refactor.refactor_warnings:
                console.print(f"  {refactor.rule_name}", style="yellow")
                for warning in refactor.refactor_warnings:
                    console.print(f"    Warning: {warning}", style="red")


@dataclass
class PythonRefactorResult:
    dry_run: bool
    file_path: Path
    refactored_content: str
    original_content: str
    refactors: list[PythonRuleRefactorResult]
    has_warnings: bool = False

    @property
    def refactored(self) -> bool:
        return any(r.refactored for r in self.refactors)

    def apply_changeset(self, func: Callable, config: Any) -> None:
        content = PythonContent(
            original_str=self.original_content,
            current_str=self.refactored_content,
        )
        result = func(content, config)
        self.refactors.append(result)
        if result.refactored:
            self.refactored_content = result.refactored_content
        if result.refactor_warnings:
            self.has_warnings = True

    def update_python_file(self) -> None:
        """Update the Python file with the refactored content"""
        Path(self.file_path).write_text(self.refactored_content)

    def print_to_console(self, json_output: bool = True):
        if not self.refactored and not self.has_warnings:
            return

        if json_output:
            flattened_refactors = []
            for refactor in self.refactors:
                if refactor.refactored:
                    flattened_refactors.extend(refactor.to_dict()["deprecation_refactors"])

            flattened_warnings = []
            for refactor in self.refactors:
                if refactor.refactor_warnings:
                    flattened_warnings.extend(refactor.refactor_warnings)

            to_print = {
                "mode": "dry_run" if self.dry_run else "applied",
                "file_path": str(self.file_path),
                "refactors": flattened_refactors,
                "warnings": flattened_warnings,
            }
            print(json.dumps(to_print))
            return

        console.print(
            f"\n{'DRY RUN - NOT APPLIED: ' if self.dry_run else ''}Refactored {self.file_path}:",
            style="green",
        )
        for refactor in self.refactors:
            if refactor.refactored:
                console.print(f"  {refactor.rule_name}", style="yellow")

                for log in refactor.refactor_logs:
                    console.print(f"    {log}")

                for warning in refactor.refactor_warnings:
                    console.print(f"    Warning: {warning}", style="red")
            elif refactor.refactor_warnings:
                console.print(f"  {refactor.rule_name}", style="yellow")
                for warning in refactor.refactor_warnings:
                    console.print(f"    Warning: {warning}", style="red")
