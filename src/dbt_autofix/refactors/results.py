import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from rich.console import Console

from dbt_autofix.refactors.fancy_quotes_utils import restore_fancy_quotes

console = Console()


@dataclass
class DbtDeprecationRefactor:
    log: str
    deprecation: Optional[str] = None

    def to_dict(self) -> dict:
        ret_dict = {"deprecation": self.deprecation, "log": self.log}

        return ret_dict


@dataclass
class YMLRuleRefactorResult:
    rule_name: str
    refactored: bool
    refactored_yaml: str
    original_yaml: str
    deprecation_refactors: list[DbtDeprecationRefactor]

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
class YMLRefactorResult:
    dry_run: bool
    file_path: Path
    refactored: bool
    refactored_yaml: str
    original_yaml: str
    refactors: list[YMLRuleRefactorResult]

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
            print(json.dumps(to_print))  # noqa: T201
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
class SQLRefactorResult:
    dry_run: bool
    file_path: Path
    refactored: bool
    refactored_file_path: Path
    refactored_content: str
    original_content: str
    refactors: list[SQLRuleRefactorResult]
    has_warnings: bool = False

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
            print(json.dumps(to_print))  # noqa: T201
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
