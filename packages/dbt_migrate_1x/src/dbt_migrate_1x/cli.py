"""Standalone CLI entry point for `dbt-migrate-1x`.

Shipped as its own uv workspace package rather than a `dbt-autofix` subcommand so it can be
invoked directly (e.g. `uv run dbt-migrate-1x`) without going through the main `dbt-autofix` CLI.
"""

import json
from pathlib import Path
from typing import List, Optional

import typer
from rich import print
from rich.console import Console
from typing_extensions import Annotated

from dbt_autofix.refactor import apply_changesets
from dbt_migrate_1x.migrate_1x import MAX_VERSION, MIN_VERSION, migrate_1x_all_files

error_console = Console(stderr=True)

app = typer.Typer(
    help="Apply deterministic fixes for changes introduced between dbt 1.x minor versions",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

current_dir = Path.cwd()


@app.command()
def migrate_1x(
    path: Annotated[
        Path, typer.Option("--path", "-p", "--project-dir", help="The path to the dbt project")
    ] = current_dir,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="In dry run mode, do not apply changes")] = False,
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output in JSON format")] = False,
    select: Annotated[
        Optional[List[str]], typer.Option("--select", "-s", help="Select specific paths to refactor")
    ] = None,
    from_version: Annotated[
        str, typer.Option("--from", help="The dbt version you are migrating from (major.minor, e.g. 1.3)")
    ] = MIN_VERSION,
    to_version: Annotated[
        str, typer.Option("--to", help="The dbt version you are migrating to (major.minor, e.g. 1.12)")
    ] = MAX_VERSION,
):
    try:
        yaml_results, sql_results = migrate_1x_all_files(path, dry_run, select, from_version, to_version)
    except ValueError as e:
        raise typer.BadParameter(str(e))

    if dry_run:
        if not json_output:
            error_console.print("[red]-- Dry run mode, not applying changes --[/red]")
        for changeset in yaml_results:
            if changeset.refactored:
                changeset.print_to_console(json_output)
        for changeset in sql_results:
            if changeset.refactored:
                changeset.print_to_console(json_output)
    else:
        apply_changesets(yaml_results, sql_results, [], json_output)

    if json_output:
        print(json.dumps({"mode": "complete"}))


if __name__ == "__main__":
    app()
