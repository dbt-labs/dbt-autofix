import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from rich.console import Console
from rich.table import Table

from dbt_autofix.sql_function_extractor import ModelFunctionScan

console = Console()


class StaticAnalysisLevel(str, Enum):
    STRICT = "strict"
    BASELINE = "baseline"
    OFF = "off"


@dataclass
class ModelAnalysisResult:
    model_path: Path
    unsupported_functions: set[str]

    def to_dict(self) -> dict:
        return {
            "model_path": str(self.model_path),
            "unsupported_functions": sorted(self.unsupported_functions),
        }


@dataclass
class ProjectAnalysisResult:
    recommended_level: StaticAnalysisLevel
    models_with_issues: list[ModelAnalysisResult] = field(default_factory=list)
    total_models_scanned: int = 0

    def to_dict(self) -> dict:
        return {
            "recommended_level": self.recommended_level.value,
            "total_models_scanned": self.total_models_scanned,
            "models_with_issues": [m.to_dict() for m in self.models_with_issues],
        }

    def print_to_console(self, json_output: bool = False) -> None:
        if json_output:
            print(json.dumps(self.to_dict()))  # noqa: T201
            return

        if not self.models_with_issues:
            console.print(
                f"[green]✓ All {self.total_models_scanned} model(s) use only Fusion-supported functions.[/green]"
            )
            console.print("[green]Recommendation: keep static_analysis: strict (default)[/green]")
            return

        console.print(
            f"[yellow]Found {len(self.models_with_issues)} model(s) with unsupported functions "
            f"(out of {self.total_models_scanned} scanned)[/yellow]\n"
        )
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Model", style="cyan")
        table.add_column("Unsupported Functions", style="red")
        for m in self.models_with_issues:
            table.add_row(str(m.model_path), ", ".join(sorted(m.unsupported_functions)))
        console.print(table)
        console.print(
            f"\n[yellow]Recommendation: set static_analysis: {self.recommended_level.value} in dbt_project.yml[/yellow]"
        )


def analyze_project(
    scans: list[ModelFunctionScan],
    unsupported_functions: set[str],
) -> ProjectAnalysisResult:
    """Cross-reference model function scans against the unsupported set."""
    models_with_issues = []
    for scan in scans:
        hits = scan.functions & unsupported_functions
        if hits:
            models_with_issues.append(
                ModelAnalysisResult(model_path=scan.model_path, unsupported_functions=hits)
            )

    level = StaticAnalysisLevel.BASELINE if models_with_issues else StaticAnalysisLevel.STRICT
    return ProjectAnalysisResult(
        recommended_level=level,
        models_with_issues=models_with_issues,
        total_models_scanned=len(scans),
    )
