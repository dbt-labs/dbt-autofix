import json
import subprocess
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.text import Text

console = Console()
error_console = Console(stderr=True)


def read_input(source: Optional[Path] = None) -> list[dict]:
    """Parse JSONL or JSON array input. Filters out sentinel/non-file entries."""
    if source is not None:
        text = source.read_text()
    else:
        text = sys.stdin.read()
    text = text.strip()
    if not text:
        return []
    if text.startswith("["):
        entries = json.loads(text)
    else:
        entries = [json.loads(line) for line in text.splitlines() if line.strip()]
    return [e for e in entries if "file_path" in e]


def _get_file_lines(file_path: Path) -> Optional[list[str]]:
    try:
        return file_path.read_text().splitlines()
    except (OSError, IOError):
        return None


def _get_git_original_lines(file_path: Path) -> Optional[list[str]]:
    """Retrieve the HEAD version of a file from git."""
    try:
        git_root_result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            cwd=file_path.parent,
            check=False,
        )
        if git_root_result.returncode != 0:
            return None
        git_root = Path(git_root_result.stdout.strip())
        relative_path = file_path.resolve().relative_to(git_root)
        result = subprocess.run(
            ["git", "show", f"HEAD:{relative_path}"],
            capture_output=True,
            text=True,
            cwd=git_root,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.splitlines()
    except (OSError, FileNotFoundError, ValueError):
        pass
    return None


def _render_location_block(
    lines: list[str],
    location: dict,
    context: int,
    label: str,
    label_style: str,
) -> None:
    line_num = location["line"]  # 1-based
    start = location.get("start")
    end = location.get("end")
    total_lines = len(lines)

    first_line = max(1, line_num - context)
    last_line = min(total_lines, line_num + context)
    line_num_width = len(str(last_line))

    col_info = f", col {start}-{end}" if start is not None and end is not None else ""
    console.print(f"        {label} (line {line_num}{col_info}):", style=label_style)

    for i in range(first_line, last_line + 1):
        line_content = lines[i - 1] if i <= total_lines else ""
        prefix = f"          {str(i).rjust(line_num_width)}:   "
        t = Text(prefix)
        t.append(line_content)
        console.print(t)

        if i == line_num and start is not None and end is not None:
            underscore = " " * (len(prefix) + start) + "^" * max(1, end - start)
            console.print(underscore, style="bold red")

    console.print()


def _display_file_refactors(
    file_path_str: str,
    refactors: list[dict],
    mode: str,
    original_lines: Optional[list[str]],
    edited_lines: Optional[list[str]],
    context: int,
) -> None:
    console.print(f"\n=== {file_path_str} ===", style="bold green")
    total = len(refactors)

    for idx, refactor in enumerate(refactors):
        change_type = refactor.get("change_type", "")
        log = refactor.get("log", "")
        original_location = refactor.get("original_location")
        edited_location = refactor.get("edited_location")

        console.print(f"\n  [{idx + 1}/{total}] ChangeType: {change_type}", style="yellow")
        console.print(f"        Log: {log}")
        console.print()

        if original_location:
            if original_lines is not None:
                line_num = original_location.get("line", 1)
                if 1 <= line_num <= len(original_lines):
                    _render_location_block(original_lines, original_location, context, "Original", "cyan")
                else:
                    console.print(
                        f"        [cyan]Original[/cyan] (line {line_num}): [dim]line out of bounds ({len(original_lines)} total)[/dim]"
                    )
            else:
                console.print(
                    f"        [cyan]Original[/cyan] (line {original_location.get('line')}): [dim]file not available[/dim]"
                )

        if edited_location:
            if edited_lines is not None:
                line_num = edited_location.get("line", 1)
                if 1 <= line_num <= len(edited_lines):
                    _render_location_block(edited_lines, edited_location, context, "Edited", "magenta")
                else:
                    console.print(
                        f"        [magenta]Edited[/magenta] (line {line_num}): [dim]line out of bounds ({len(edited_lines)} total)[/dim]"
                    )
            elif mode == "dry_run":
                console.print("        [dim]Edited location available only after changes are applied[/dim]")
            else:
                console.print(
                    f"        [magenta]Edited[/magenta] (line {edited_location.get('line')}): [dim]file not available[/dim]"
                )

    console.rule(style="dim")


def run_verify_locations(
    input_source: Optional[Path],
    path: Optional[Path],
    original_base: Optional[Path],
    context_lines: int,
    change_types: Optional[list[str]] = None,
    file_filter: Optional[str] = None,
) -> None:
    """Display autofix locations in context from a JSONL file, stdin, or a live dry-run."""
    if path is not None and input_source is None:
        from dbt_autofix.refactor import changeset_all_files
        from dbt_autofix.retrieve_schemas import SchemaSpecs

        schema_specs = SchemaSpecs(None, False)
        yaml_results, sql_results, python_results = changeset_all_files(path, schema_specs, dry_run=True)

        logs_io = StringIO()
        with redirect_stdout(logs_io):
            for result in [*yaml_results, *sql_results, *python_results]:
                if result.refactored:
                    result.print_to_console(json_output=True)

        text = logs_io.getvalue().strip()
        if not text:
            console.print("[dim]No refactors found.[/dim]")
            return
        entries = [json.loads(line) for line in text.splitlines() if line.strip()]
        entries = [e for e in entries if "file_path" in e]
    else:
        entries = read_input(input_source)

    if not entries:
        console.print("[dim]No refactors found in input.[/dim]")
        return

    base_path = path or Path.cwd()

    for entry in entries:
        file_path_str = entry.get("file_path", "")
        mode = entry.get("mode", "applied")
        refactors = entry.get("refactors", [])

        if file_filter and file_filter not in file_path_str:
            continue

        if change_types:
            refactors = [r for r in refactors if r.get("change_type") in change_types]

        if not refactors:
            continue

        file_path = Path(file_path_str)
        if not file_path.is_absolute():
            file_path = (base_path / file_path).resolve()

        if original_base is not None:
            orig_file = Path(file_path_str)
            if not orig_file.is_absolute():
                orig_file = (original_base / orig_file).resolve()
            original_lines = _get_file_lines(orig_file)
            if original_lines is None:
                error_console.print(f"[yellow]Warning: could not read original file: {orig_file}[/yellow]")
        elif mode == "applied":
            git_lines = _get_git_original_lines(file_path)
            original_lines = git_lines if git_lines is not None else _get_file_lines(file_path)
        else:
            original_lines = _get_file_lines(file_path)

        if original_lines is None and original_base is None:
            error_console.print(f"[yellow]Warning: could not read file: {file_path}[/yellow]")

        edited_lines = _get_file_lines(file_path) if mode == "applied" else None

        _display_file_refactors(file_path_str, refactors, mode, original_lines, edited_lines, context_lines)
