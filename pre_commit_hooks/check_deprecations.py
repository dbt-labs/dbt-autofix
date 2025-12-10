import argparse
import sys
from pathlib import Path
from typing import List, Optional

from dbt_autofix.refactor import apply_changesets, changeset_all_sql_yml_files, get_dbt_files_paths
from dbt_autofix.refactors.results import SQLRefactorResult, YMLRefactorResult
from dbt_autofix.retrieve_schemas import SchemaSpecs

VALID_DBT_EXTENSIONS = {".sql", ".yml", ".yaml"}


def is_relevant_dbt_file(file_path: Path, dbt_paths: dict, root_path: Path = Path.cwd()) -> bool:
    """Check if a file is a relevant dbt file using the project's actual configuration."""
    if file_path.suffix not in VALID_DBT_EXTENSIONS:
        return False

    if file_path.name == "dbt_project.yml":
        return True

    # Normalize to posix paths for cross-platform comparison
    # If file_path relative, resolve to root_path
    if not file_path.is_absolute():
        file_path_posix = (root_path / file_path).resolve().as_posix()
    else:
        file_path_posix = file_path.resolve().as_posix()

    for dbt_path in dbt_paths.keys():
        dbt_path_posix = (root_path / dbt_path).resolve().as_posix()
        # Check if file is exactly the dbt_path or is within the directory
        if file_path_posix == dbt_path_posix or file_path_posix.startswith(dbt_path_posix + "/"):
            return True

    return False


def filter_relevant_files(filenames: List[str], root_path: Path = Path.cwd()) -> List[str]:
    """Filter list of filenames to only include relevant dbt files."""
    if not filenames:
        return []

    dbt_paths = get_dbt_files_paths(root_path, include_packages=False)
    relevant_files = [f for f in filenames if is_relevant_dbt_file(Path(f), dbt_paths, root_path)]

    return relevant_files


def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    This function acts as an adapter between pre-commit's interface (which passes
    filenames as positional arguments) and dbt-autofix's internal API (which expects
    a list via the 'select' parameter). We can't use the dbt-autofix CLI directly
    because we need to:
    1. Convert positional filenames to the internal select parameter
    2. Filter only relevant dbt files before processing
    3. Return appropriate exit codes based on whether changes were found
    """
    parser = argparse.ArgumentParser(description="Check for dbt deprecations in staged files")
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Filenames to check (passed by pre-commit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run in dry-run mode (check only, do not apply fixes)",
    )
    parser.add_argument(
        "--behavior-change",
        action="store_true",
        help="Run fixes to deprecations that may require a behavior change",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all fixes, including those that may require a behavior change",
    )
    parser.add_argument(
        "--include-packages",
        action="store_true",
        help="Include packages in the refactoring",
    )
    parser.add_argument(
        "--semantic-layer",
        action="store_true",
        help="Run fixes to semantic layer deprecations",
    )
    parser.add_argument(
        "--exclude-dbt-project-keys",
        action="store_true",
        help="Exclude dbt_project.yml keys from refactoring",
    )
    parser.add_argument(
        "--path",
        "-p",
        type=str,
        default=".",
        help="The path to the dbt project",
    )

    return parser.parse_args(argv)


def has_any_changes(yaml_results: List[YMLRefactorResult], sql_results: List[SQLRefactorResult]) -> bool:
    """Check if any changesets contain refactoring changes."""
    return any(c.refactored for c in yaml_results) or any(c.refactored for c in sql_results)


def main(argv: Optional[List[str]] = None) -> int:
    """Run dbt-autofix deprecations check on staged files."""
    args = parse_arguments(argv)

    path = Path(args.path)
    select = filter_relevant_files(args.filenames, root_path=path)

    if not select:
        return 0  # No relevant files to check

    schema_specs = SchemaSpecs(version=None)
    changesets = changeset_all_sql_yml_files(
        path=path,
        schema_specs=schema_specs,
        dry_run=args.dry_run,
        exclude_dbt_project_keys=args.exclude_dbt_project_keys,
        select=select,
        include_packages=args.include_packages,
        behavior_change=args.behavior_change,
        all=args.all,
        semantic_layer=args.semantic_layer,
    )

    yaml_results, sql_results = changesets

    if args.dry_run:
        for changeset in yaml_results:
            if changeset.refactored:
                changeset.print_to_console(json_output=False)
        for changeset in sql_results:
            if changeset.refactored:
                changeset.print_to_console(json_output=False)
    else:
        # Fix mode
        apply_changesets(yaml_results, sql_results, json_output=False)

    has_changes = has_any_changes(yaml_results, sql_results)
    if has_changes:
        return 1  # For pre-commit: changes found (dry-run) or made (fix mode)

    return 0  # No changes needed


if __name__ == "__main__":
    sys.exit(main())
