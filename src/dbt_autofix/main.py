import json
from importlib.metadata import version
from pathlib import Path
from typing import List, Optional

import typer
from rich import print
from rich.console import Console
from typing_extensions import Annotated

from dbt_autofix.dbt_api import update_jobs
from dbt_autofix.duplicate_keys import find_duplicate_keys, print_duplicate_keys
from dbt_autofix.fields_properties_configs import print_matrix
from dbt_autofix.function_support import get_unsupported_functions, load_function_support
from dbt_autofix.package_upgrade import (
    PackageUpgradeResult,
    PackageVersionUpgradeResult,
    check_for_package_upgrades,
    generate_package_dependencies,
    upgrade_package_versions,
)
from dbt_autofix.packages.dbt_package_file import DbtPackageFile
from dbt_autofix.refactor import apply_changesets, changeset_all_sql_yml_files
from dbt_autofix.retrieve_schemas import SchemaSpecs
from dbt_autofix.manifest_reader import (
    get_model_infos,
    get_transitive_descendants,
    load_manifest,
    match_compiled_paths_to_unique_ids,
)
from dbt_autofix.sql_function_extractor import scan_compiled_dir
from dbt_autofix.static_analysis_report import ProjectAnalysisResult, analyze_project
from dbt_autofix.static_analysis_writer import (
    apply_baseline_to_model_schema,
    apply_baseline_to_model_sql,
    apply_strict_project_default,
)

console = Console()
error_console = Console(stderr=True)

app = typer.Typer(
    help="A tool to help clean up dbt projects",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

current_dir = Path.cwd()


@app.command(name="list-yaml-duplicates")
def identify_duplicate_keys(
    path: Annotated[Path, typer.Option("--path", "-p", help="The path to the dbt project")] = current_dir,
):
    print(f"[green]Identifying duplicates in {path}[/green]\n")
    project_duplicates, package_duplicates = find_duplicate_keys(path)
    print_duplicate_keys(project_duplicates, package_duplicates)


@app.command(name="packages")
def upgrade_packages(
    path: Annotated[Path, typer.Option("--path", "-p", help="The path to the dbt project")] = current_dir,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="In dry run mode, do not apply changes")] = False,
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output in JSON format")] = False,
    force_upgrade: Annotated[
        bool,
        typer.Option(
            "--force-upgrade", "-f", help="Override package version config when upgrading to Fusion-compatible versions"
        ),
    ] = False,
):
    if not path.is_dir() or not path.exists():
        error_console.print("[red]-- The directory specified in --path does not exist --[/red]")
        return

    console.print(f"[green]Identifying packages with available upgrades in {path}[/green]\n")
    try:
        deps_file: Optional[DbtPackageFile] = generate_package_dependencies(path)
        if not deps_file:
            error_console.print("[red]-- No package dependency config found --[/red]")
            return

        if len(deps_file.package_dependencies) == 0:
            error_console.print("[red]-- No package dependencies found --[/red]")
            return

        package_upgrades: list[PackageVersionUpgradeResult] = check_for_package_upgrades(deps_file)

        packages_upgraded: PackageUpgradeResult = upgrade_package_versions(
            deps_file=deps_file,
            package_dependencies_with_upgrades=package_upgrades,
            dry_run=dry_run,
            override_pinned_version=force_upgrade,
            json_output=json_output,
        )
        packages_upgraded.print_to_console(json_output=json_output)
    except:
        error_console.print("[red]-- Package upgrade failed, please check logs for details --[/red]")
    if json_output:
        print(json.dumps({"mode": "complete"}))  # noqa: T201


@app.command(name="deprecations")
def refactor_yml(  # noqa: PLR0913
    path: Annotated[Path, typer.Option("--path", "-p", help="The path to the dbt project")] = current_dir,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="In dry run mode, do not apply changes")] = False,
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output in JSON format")] = False,
    exclude_dbt_project_keys: Annotated[
        bool, typer.Option("--exclude-dbt-project-keys", "-e", help="Exclude specific dbt project keys", hidden=True)
    ] = False,
    json_schema_version: Annotated[
        Optional[str], typer.Option("--json-schema-version", help="Specific version of the JSON schema to use")
    ] = None,
    select: Annotated[
        Optional[List[str]], typer.Option("--select", "-s", help="Select specific paths to refactor")
    ] = None,
    include_packages: Annotated[
        bool,
        typer.Option(
            "--include-packages", "-i", help="Include all packages (private or public/hub) in the refactoring"
        ),
    ] = False,
    include_private_packages: Annotated[
        bool,
        typer.Option(
            "--include-private-packages",
            "-ip",
            help="Include only private packages (non-hub packages) in the refactoring",
        ),
    ] = False,
    behavior_change: Annotated[
        bool, typer.Option("--behavior-change", help="Run fixes to deprecations that may require a behavior change")
    ] = False,
    all: Annotated[
        bool, typer.Option("--all", help="Run all fixes, including those that may require a behavior change")
    ] = False,
    semantic_layer: Annotated[bool, typer.Option("--semantic-layer", help="Run fixes to semantic layer")] = False,
    disable_ssl_verification: Annotated[
        bool, typer.Option("--disable-ssl-verification", help="Disable SSL verification", hidden=True)
    ] = False,
):
    if semantic_layer and include_packages:
        raise typer.BadParameter("--include-packages is not supported with --semantic-layer")

    schema_specs = SchemaSpecs(json_schema_version, disable_ssl_verification)

    changesets = changeset_all_sql_yml_files(
        path,
        schema_specs,
        dry_run,
        exclude_dbt_project_keys,
        select,
        include_packages,
        include_private_packages,
        behavior_change,
        all,
        semantic_layer,
    )
    yaml_results, sql_results = changesets
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
        apply_changesets(yaml_results, sql_results, json_output)

    if json_output:
        print(json.dumps({"mode": "complete"}))  # noqa: T201


@app.command(name="jobs")
def jobs(  # noqa: PLR0913
    account_id: Annotated[
        int, typer.Option("--account-id", "-a", help="The account ID to use", envvar="DBT_ACCOUNT_ID")
    ],
    api_key: Annotated[
        str, typer.Option("--api-key", "-k", help="The user token or service token to use", envvar="DBT_API_KEY")
    ],
    base_url: Annotated[
        str, typer.Option("--base-url", "-b", help="The base URL to use", envvar="DBT_BASE_URL")
    ] = "https://cloud.getdbt.com",
    disable_ssl_verification: Annotated[
        bool, typer.Option("--disable-ssl-verification", "-s", help="Disable SSL verification", hidden=True)
    ] = False,
    project_ids: Annotated[
        Optional[List[int]], typer.Option("--project-ids", "-p", help="The project IDs to use")
    ] = None,
    environment_ids: Annotated[
        Optional[List[int]], typer.Option("--environment-ids", "-e", help="The environment IDs to use")
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="In dry run mode, do not apply changes")] = False,
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output in JSON format")] = False,
    behavior_change: Annotated[
        bool, typer.Option("--behavior-change", help="Run fixes to deprecations that may require a behavior change")
    ] = False,
):
    update_jobs(
        account_id,
        api_key,
        base_url,
        disable_ssl_verification,
        project_ids,
        environment_ids,
        dry_run,
        json_output,
        behavior_change,
    )


@app.command(hidden=True)
def print_fields_matrix(
    json_schema_version: Annotated[
        Optional[str], typer.Option("--json-schema-version", help="Specific version of the JSON schema to use")
    ] = None,
    disable_ssl_verification: Annotated[
        bool, typer.Option("--disable-ssl-verification", help="Disable SSL verification", hidden=True)
    ] = False,
):
    print_matrix(json_schema_version, disable_ssl_verification)


@app.command(name="fusion-static-analysis")
def fusion_static_analysis(  # noqa: PLR0913
    path: Annotated[Path, typer.Option("--path", "-p", help="Path to the dbt project root")] = current_dir,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="Report findings without modifying files")] = False,
    apply: Annotated[
        bool, typer.Option("--apply", help="Write recommended static_analysis level to dbt_project.yml")
    ] = False,
    json_output: Annotated[bool, typer.Option("--json", "-j", help="Output results as JSON")] = False,
    select: Annotated[
        Optional[List[str]], typer.Option("--select", "-s", help="Limit analysis to specific model paths or globs")
    ] = None,
    adapter: Annotated[
        str, typer.Option("--adapter", help="Target adapter for function compatibility lookup")
    ] = "snowflake",
    disable_ssl_verification: Annotated[
        bool, typer.Option("--disable-ssl-verification", help="Disable SSL verification", hidden=True)
    ] = False,
):
    """Check compiled SQL models for functions unsupported in Fusion strict static analysis."""
    if not path.is_dir() or not path.exists():
        error_console.print("[red]-- The directory specified in --path does not exist --[/red]")
        raise typer.Exit(code=1)

    compiled_root = path / "target" / "compiled"
    if not compiled_root.exists():
        error_console.print(
            "[red]-- No target/compiled/ directory found. Run `dbtf compile` or `dbt compile` first. --[/red]"
        )
        raise typer.Exit(code=1)

    if not json_output:
        console.print(f"[green]Scanning compiled SQL in {compiled_root}[/green]\n")

    support_data = load_function_support(adapter=adapter)
    unsupported = get_unsupported_functions(support_data)

    scans = scan_compiled_dir(path, select=select)
    result: ProjectAnalysisResult = analyze_project(scans, unsupported)

    result.print_to_console(json_output=json_output)

    if result.models_with_issues and apply and not dry_run:
        # Load manifest to resolve compiled paths → unique IDs and find DAG descendants
        try:
            manifest = load_manifest(path)
        except FileNotFoundError:
            error_console.print(
                "[red]-- --apply requires target/manifest.json. Run `dbtf compile` first. --[/red]"
            )
            raise typer.Exit(code=1)

        model_infos = get_model_infos(manifest)
        offending_compiled = [m.model_path for m in result.models_with_issues]
        path_to_uid = match_compiled_paths_to_unique_ids(manifest, offending_compiled, path)
        offending_uids = set(path_to_uid.values())
        descendant_uids = get_transitive_descendants(manifest, offending_uids)
        baseline_uids = offending_uids | descendant_uids

        # 1. Strict as the project-wide default
        apply_strict_project_default(path)

        # 2. Baseline per-model for offenders + their descendants
        applied_names = []
        for uid in baseline_uids:
            info = model_infos.get(uid)
            if not info:
                continue
            if info.patch_path:
                apply_baseline_to_model_schema(path, info.patch_path, info.name)
            else:
                apply_baseline_to_model_sql(path, info.original_file_path)
            applied_names.append(info.name)

        if not json_output:
            console.print(
                "\n[green]Applied:[/green]"
                "\n  +static_analysis: strict → dbt_project.yml (project default)"
            )
            for name in sorted(applied_names):
                console.print(f"  static_analysis: baseline → {name}")

    if json_output:
        print(json.dumps({"mode": "complete"}))

    if result.models_with_issues:
        raise typer.Exit(code=1)


def version_callback(show_version: bool):
    if show_version:
        typer.echo(f"dbt-autofix {version('dbt-autofix')}")
        raise typer.Exit()


if __name__ == "__main__":
    app()


@app.callback()
def main(
    debug: bool = typer.Option(
        False, "--debug", help="Enable debug mode with pretty exceptions", envvar="DBT_AUTOFIX_DEBUG"
    ),
    version: bool = typer.Option(None, "--version", "-v", callback=version_callback),
):
    if debug:
        app.pretty_exceptions_enable = True
