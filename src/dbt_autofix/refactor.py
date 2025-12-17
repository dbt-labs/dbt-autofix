from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import yamllint.linter
from rich.console import Console
from yaml import safe_load

from dbt_autofix.hub_packages import should_skip_package
from dbt_autofix.refactors.changesets.dbt_project_yml import (
    changeset_dbt_project_flip_behavior_flags,
    changeset_dbt_project_flip_test_arguments_behavior_flag,
    changeset_dbt_project_prefix_plus_for_config,
    changeset_dbt_project_remove_deprecated_config,
    changeset_fix_space_after_plus,
)
from dbt_autofix.refactors.changesets.dbt_schema_yml import (
    changeset_owner_properties_yml_str,
    changeset_refactor_yml_str,
    changeset_remove_duplicate_models,
    changeset_remove_extra_tabs,
    changeset_remove_indentation_version,
    changeset_remove_tab_only_lines,
    changeset_replace_fancy_quotes,
    changeset_replace_non_alpha_underscores_in_name_values,
)
from dbt_autofix.refactors.changesets.dbt_schema_yml_semantic_layer import (
    changeset_add_metrics_for_measures,
    changeset_delete_top_level_semantic_models,
    changeset_merge_complex_metrics_with_models,
    changeset_merge_semantic_models_with_models,
    changeset_merge_simple_metrics_with_models,
    changeset_migrate_or_delete_top_level_metrics,
)
from dbt_autofix.refactors.changesets.dbt_sql import (
    refactor_custom_configs_to_meta_sql,
    remove_unmatched_endings,
    rename_sql_file_names_with_spaces,
)
from dbt_autofix.refactors.changesets.dbt_sql_improved import (
    move_custom_config_access_to_meta_sql_improved,
)
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    SQLRefactorResult,
    YMLRefactorResult,
    YMLRuleRefactorResult,
)
from dbt_autofix.refactors.yml import DbtYAML, yaml_config
from dbt_autofix.retrieve_schemas import (
    SchemaSpecs,
)
from dbt_autofix.semantic_definitions import SemanticDefinitions

error_console = Console(stderr=True)

config = """
rules:
  key-duplicates: enable
"""


def process_yaml_files_except_dbt_project(
    root_path: Path,
    model_paths: Iterable[str],
    schema_specs: SchemaSpecs,
    dry_run: bool = False,
    select: Optional[List[str]] = None,
    behavior_change: bool = False,
    all: bool = False,
    semantic_definitions: Optional[SemanticDefinitions] = None,
) -> List[YMLRefactorResult]:
    """Process all YAML files in the project.

    Args:
        path: Project root path
        model_paths: Paths to process
        schema_specs: The schema specifications to use
        dry_run: Whether to perform a dry run
        select: Optional list of paths to select
        behavior_change: Whether to apply fixes that may lead to behavior changes
        all: Whether to run all fixes, including those that may require a behavior change
    """
    file_name_to_yaml_results: Dict[str, YMLRefactorResult] = {}

    behavior_change_rules = [
        (changeset_replace_non_alpha_underscores_in_name_values, schema_specs),
    ]
    safe_change_rules = [
        (changeset_replace_fancy_quotes, None),
        (changeset_remove_tab_only_lines, None),
        (changeset_remove_indentation_version, None),
        (changeset_remove_extra_tabs, None),
        (changeset_remove_duplicate_keys, None),
        (changeset_remove_duplicate_models, None),
        (changeset_refactor_yml_str, schema_specs),
        (changeset_owner_properties_yml_str, schema_specs),
    ]
    all_rules = [*safe_change_rules, *behavior_change_rules]
    changesets = all_rules if all else behavior_change_rules if behavior_change else safe_change_rules

    ordered_changesets = [
        changesets,
    ]

    # Override ordered changesets if semantic definitions are provided
    if semantic_definitions:
        # Certain changesets can only be applied after all the other changesets have been applied to all the files
        ordered_changesets = [
            [
                (changeset_merge_semantic_models_with_models, semantic_definitions),
                (changeset_merge_simple_metrics_with_models, semantic_definitions),
            ],
            [
                (changeset_add_metrics_for_measures, semantic_definitions),
            ],
            [
                (changeset_merge_complex_metrics_with_models, semantic_definitions),
            ],
            [
                (changeset_delete_top_level_semantic_models, semantic_definitions),
                (changeset_migrate_or_delete_top_level_metrics, semantic_definitions),
            ],
        ]

    def _apply_changesets(
        file_name_to_yaml_results: Dict[str, YMLRefactorResult], changesets: List[Tuple[Callable, Any]]
    ) -> None:
        for model_path in model_paths:
            yaml_files = set((root_path / Path(model_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(model_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                if skip_file(yml_file, select):
                    continue

                if str(yml_file) in file_name_to_yaml_results:
                    yml_refactor_result = file_name_to_yaml_results[str(yml_file)]
                else:
                    yml_str = yml_file.read_text()
                    yml_refactor_result = YMLRefactorResult(
                        dry_run=dry_run,
                        file_path=yml_file,
                        refactored=False,
                        refactored_yaml=yml_str,
                        original_yaml=yml_str,
                        refactors=[],
                    )
                # Apply each changeset in sequence
                try:
                    for changeset_func, changeset_args in changesets:
                        if changeset_args is None:
                            changeset_result = changeset_func(yml_refactor_result.refactored_yaml)
                        else:
                            changeset_result = changeset_func(yml_refactor_result.refactored_yaml, changeset_args)

                        if changeset_result.refactored:
                            yml_refactor_result.refactors.append(changeset_result)
                            yml_refactor_result.refactored = True
                            yml_refactor_result.refactored_yaml = changeset_result.refactored_yaml

                        file_name_to_yaml_results[str(yml_file)] = yml_refactor_result

                except Exception as e:
                    if all:
                        error_console.print(
                            f"Warning: Could not apply fixes to {yml_file}: {e.__class__.__name__}: {e}", style="yellow"
                        )
                    else:
                        error_console.print(
                            f"Error processing YAML at path {yml_file}: {e.__class__.__name__}: {e}", style="bold red"
                        )
                        exit(1)

    for changesets in ordered_changesets:
        _apply_changesets(file_name_to_yaml_results, changesets)

    return list(file_name_to_yaml_results.values())


def process_dbt_project_yml(
    root_path: Path,
    schema_specs: SchemaSpecs,
    dry_run: bool = False,
    exclude_dbt_project_keys: bool = False,
    behavior_change: bool = False,
    all: bool = False,
) -> YMLRefactorResult:
    """Process dbt_project.yml."""
    if not (root_path / "dbt_project.yml").exists():
        error_console.print(f"Error: dbt_project.yml not found in {root_path}", style="red")
        return YMLRefactorResult(
            dry_run=dry_run,
            file_path=root_path / "dbt_project.yml",
            refactored=False,
            refactored_yaml="",
            original_yaml="",
            refactors=[],
        )

    yml_str = (root_path / "dbt_project.yml").read_text()
    yml_refactor_result = YMLRefactorResult(
        dry_run=dry_run,
        file_path=root_path / "dbt_project.yml",
        refactored=False,
        refactored_yaml=yml_str,
        original_yaml=yml_str,
        refactors=[],
    )

    behavior_change_rules = [(changeset_dbt_project_flip_behavior_flags, None)]
    safe_change_rules = [
        (changeset_replace_fancy_quotes, None),
        (changeset_remove_duplicate_keys, None),
        (changeset_dbt_project_flip_test_arguments_behavior_flag, None),
        (changeset_dbt_project_remove_deprecated_config, exclude_dbt_project_keys),
        (changeset_fix_space_after_plus, schema_specs),
        (changeset_dbt_project_prefix_plus_for_config, root_path, schema_specs),
    ]
    all_rules = [*behavior_change_rules, *safe_change_rules]

    changesets = all_rules if all else behavior_change_rules if behavior_change else safe_change_rules

    for changeset_func, *changeset_args in changesets:
        if changeset_args[0] is None:
            changeset_result = changeset_func(yml_refactor_result.refactored_yaml)
        elif len(changeset_args) == 1:
            changeset_result = changeset_func(yml_refactor_result.refactored_yaml, changeset_args[0])
        else:
            changeset_result = changeset_func(yml_refactor_result.refactored_yaml, *changeset_args)

        if changeset_result.refactored:
            yml_refactor_result.refactors.append(changeset_result)
            yml_refactor_result.refactored = True
            yml_refactor_result.refactored_yaml = changeset_result.refactored_yaml

    return yml_refactor_result


def skip_file(file_path: Path, select: Optional[List[str]] = None) -> bool:
    """Skip a file if a select list is provided and the file is not in the select list"""
    if select:
        return not any([Path(select_path).resolve().as_posix() in file_path.as_posix() for select_path in select])
    else:
        return False


def process_sql_files(
    path: Path,
    sql_paths_to_node_type: Dict[str, str],
    schema_specs: SchemaSpecs,
    dry_run: bool = False,
    select: Optional[List[str]] = None,
    behavior_change: bool = False,
    all: bool = False,
) -> List[SQLRefactorResult]:
    """Process all SQL files in the given paths for unmatched endings.

    Args:
        path: Base project path
        sql_paths: Set of paths relative to project root where SQL files are located
        dry_run: Whether to perform a dry run
        select: Optional list of paths to select
        behavior_change: Whether to apply fixes that may lead to behavior change
        all: Whether to run all fixes, including those that may require a behavior change

    Returns:
        List of SQLRefactorResult for each processed file
    """
    results: List[SQLRefactorResult] = []

    behavior_change_rules = [(rename_sql_file_names_with_spaces, True, False)]
    safe_change_rules = [
        (remove_unmatched_endings, False, False),
        (refactor_custom_configs_to_meta_sql, False, True),
        (move_custom_config_access_to_meta_sql_improved, False, True),
    ]
    all_rules = [*behavior_change_rules, *safe_change_rules]

    process_sql_file_rules = all_rules if all else behavior_change_rules if behavior_change else safe_change_rules

    for sql_path, node_type in sql_paths_to_node_type.items():
        full_path = (path / sql_path).resolve()
        if not full_path.exists():
            error_console.print(f"Warning: Path {full_path} does not exist", style="yellow")
            continue

        sql_files = full_path.glob("**/*.sql")
        for sql_file in sql_files:
            if skip_file(full_path, select):
                continue

            try:
                file_refactors: List[SQLRefactorResult] = []

                original_content = sql_file.read_text()
                new_content = original_content

                new_file_path = sql_file
                for sql_file_rule, requires_file_path, requires_schema_specs in process_sql_file_rules:
                    if requires_file_path and requires_schema_specs:
                        sql_file_refactor_result = sql_file_rule(new_content, new_file_path, schema_specs, node_type)
                    elif requires_file_path:
                        sql_file_refactor_result = sql_file_rule(new_content, new_file_path)
                    elif requires_schema_specs:
                        sql_file_refactor_result = sql_file_rule(new_content, schema_specs, node_type)
                    else:
                        sql_file_refactor_result = sql_file_rule(new_content)

                    new_content = sql_file_refactor_result.refactored_content
                    new_file_path = sql_file_refactor_result.refactored_file_path or sql_file
                    file_refactors.append(sql_file_refactor_result)

                refactored = (new_content != original_content) or (new_file_path != sql_file)
                has_warnings = any([refactor.refactor_warnings for refactor in file_refactors])
                results.append(
                    SQLRefactorResult(
                        dry_run=dry_run,
                        file_path=sql_file,
                        refactored=refactored,
                        refactored_content=new_content,
                        original_content=original_content,
                        refactors=file_refactors,
                        refactored_file_path=new_file_path,
                        has_warnings=has_warnings,
                    )
                )
            except Exception as e:
                if all:
                    error_console.print(
                        f"Warning: Could not apply fixes to {sql_file}: {e.__class__.__name__}: {e}", style="yellow"
                    )
                else:
                    error_console.print(f"Error processing {sql_file}: {e.__class__.__name__}: {e}", style="bold red")

    return results


def changeset_remove_duplicate_keys(yml_str: str) -> YMLRuleRefactorResult:
    """Removes duplicate keys in the YAML files, keeping the first occurence only.

    The drawback of keeping the first occurence is that we need to use PyYAML and then lose all the comments that were in the file
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    for p in yamllint.linter.run(yml_str, yaml_config):
        if p.rule == "key-duplicates":
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Found duplicate keys: line {p.line} - {p.desc}", deprecation="DuplicateYAMLKeysDeprecation"
                )
            )

    if refactored:
        import yaml

        # we use dump from ruamel to keep indentation style but this loses quite a bit of formatting though
        refactored_yaml = DbtYAML().dump_to_string(yaml.safe_load(yml_str))  # type: ignore
    else:
        refactored_yaml = yml_str

    return YMLRuleRefactorResult(
        rule_name="remove_duplicate_keys",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def get_dbt_files_paths(
    root_path: Path, include_packages: bool = False, include_private_packages: bool = False
) -> Dict[str, str]:
    """Get model and macro paths from dbt_project.yml.

    Args:
        root_path: Project root path
        include_packages: Whether to include packages in the refactoring
        include_private_packages: Whether to include private packages (non-hub packages)

    Returns:
        A list of paths to the models, macros, tests, analyses, and snapshots
    """

    if not (root_path / "dbt_project.yml").exists():
        error_console.print(f"Error: dbt_project.yml not found in {root_path}", style="red")
        return {}

    with open(root_path / "dbt_project.yml", "r") as f:
        project_config = safe_load(f)

    if project_config is None:
        return {}

    key_to_paths = {
        "model-paths": project_config.get("model-paths", ["models"]),
        "seed-paths": project_config.get("seed-paths", ["seeds"]),
        "macro-paths": project_config.get("macro-paths", ["macros"]),
        "test-paths": project_config.get("test-paths", ["tests"]),
        "analysis-paths": project_config.get("analysis-paths", ["analyses"]),
        "snapshot-paths": project_config.get("snapshot-paths", ["snapshots"]),
    }

    key_to_node_type = {
        "model-paths": "models",
        "seed-paths": "seeds",
        "macro-paths": "macros",
        "test-paths": "tests",
        "analysis-paths": "analyses",
        "snapshot-paths": "snapshots",
    }

    path_to_node_type = {}

    for key, paths in key_to_paths.items():
        if not isinstance(paths, list):
            error_console.print(f"Warning: Paths '{paths}' for '{key}' cannot be autofixed", style="yellow")
            continue
        for path in paths:
            path_to_node_type[str(path)] = key_to_node_type[key]

    if include_packages or include_private_packages:
        packages_path = project_config.get("packages-paths", "dbt_packages")
        packages_dir = root_path / packages_path

        if packages_dir.exists():
            for package_folder in packages_dir.iterdir():
                if package_folder.is_dir():
                    # Check if we should skip this package based on hub status
                    if should_skip_package(package_folder, include_private_packages):
                        continue

                    package_dbt_project = package_folder / "dbt_project.yml"
                    if package_dbt_project.exists():
                        with open(package_dbt_project, "r") as f:
                            package_config = safe_load(f)

                        package_model_paths = package_config.get("model-paths", ["models"])
                        package_seed_paths = package_config.get("seed-paths", ["seeds"])
                        package_macro_paths = package_config.get("macro-paths", ["macros"])
                        package_test_paths = package_config.get("test-paths", ["tests"])
                        package_analysis_paths = package_config.get("analysis-paths", ["analyses"])
                        package_snapshot_paths = package_config.get("snapshot-paths", ["snapshots"])

                        # Combine package folder path with each path type
                        for model_path in package_model_paths:
                            path_to_node_type[str(package_folder / model_path)] = "models"
                        for seed_path in package_seed_paths:
                            path_to_node_type[str(package_folder / seed_path)] = "seeds"
                        for macro_path in package_macro_paths:
                            path_to_node_type[str(package_folder / macro_path)] = "macros"
                        for test_path in package_test_paths:
                            path_to_node_type[str(package_folder / test_path)] = "tests"
                        for analysis_path in package_analysis_paths:
                            path_to_node_type[str(package_folder / analysis_path)] = "analyses"
                        for snapshot_path in package_snapshot_paths:
                            path_to_node_type[str(package_folder / snapshot_path)] = "snapshots"

    return path_to_node_type


def get_dbt_roots_paths(
    root_path: Path, include_packages: bool = False, include_private_packages: bool = False
) -> Set[str]:
    """Get all dbt root paths, the main one and the ones under dbt_packages directory if we want to include packages.

    Args:
        root_path: Project root path
        include_packages: Whether to include packages
        include_private_packages: Whether to include private packages (non-hub packages)

    Returns:
        Set of package folder paths as strings
    """
    dbt_roots_paths = {str(root_path)}
    dbt_packages_path = root_path / "dbt_packages"

    if (include_packages or include_private_packages) and dbt_packages_path.exists() and dbt_packages_path.is_dir():
        for package_folder in dbt_packages_path.iterdir():
            if package_folder.is_dir():
                # Check if we should skip this package based on hub status
                if should_skip_package(package_folder, include_private_packages):
                    continue

                dbt_roots_paths.add(str(package_folder))

    return dbt_roots_paths


def changeset_all_sql_yml_files(  # noqa: PLR0913
    path: Path,
    schema_specs: SchemaSpecs,
    dry_run: bool = False,
    exclude_dbt_project_keys: bool = False,
    select: Optional[List[str]] = None,
    include_packages: bool = False,
    include_private_packages: bool = False,
    behavior_change: bool = False,
    all: bool = False,
    semantic_layer: bool = False,
) -> Tuple[List[YMLRefactorResult], List[SQLRefactorResult]]:
    """Process all YAML files and SQL files in the project.

    Args:
        path: Project root path
        schema_specs: The schema specifications to use
        dry_run: Whether to perform a dry run
        exclude_dbt_project_keys: Whether to exclude dbt project keys
        select: List of paths to select
        include_packages: Whether to include packages in the refactoring
        include_private_packages: Whether to include private packages (non-hub packages)
        behavior_change: Whether to apply fixes that may lead to behavior changes
        all: Whether to run all fixes, including those that may require a behavior change
        semantic_layer: Whether to run fixes to semantic layer

    Returns:
        Tuple containing:
        - List of YAML refactor results
        - List of SQL refactor results
    """
    # Get dbt root paths first (doesn't parse dbt_project.yml)
    dbt_roots_paths = get_dbt_roots_paths(path, include_packages, include_private_packages)

    # Process dbt_project.yml FIRST before we try to read it for paths
    # This ensures fancy quotes and other issues are fixed before parsing
    dbt_project_yml_results = []
    if not semantic_layer:
        for dbt_root_path in dbt_roots_paths:
            result = process_dbt_project_yml(
                Path(dbt_root_path), schema_specs, dry_run, exclude_dbt_project_keys, behavior_change, all
            )
            dbt_project_yml_results.append(result)
            # If not dry run, write the changes immediately before reading the file
            if not dry_run and result.refactored:
                result.update_yaml_file()

    # Now we can safely read dbt_project.yml to get paths
    dbt_paths_to_node_type = get_dbt_files_paths(path, include_packages, include_private_packages)
    dbt_paths = list(dbt_paths_to_node_type.keys())

    sql_results = process_sql_files(path, dbt_paths_to_node_type, schema_specs, dry_run, select, behavior_change, all)

    # Process YAML files
    semantic_definitions = SemanticDefinitions(path, dbt_paths) if semantic_layer else None
    yaml_results = process_yaml_files_except_dbt_project(
        path, dbt_paths, schema_specs, dry_run, select, behavior_change, all, semantic_definitions
    )

    return [*yaml_results, *dbt_project_yml_results], sql_results


def apply_changesets(
    yaml_results: List[YMLRefactorResult],
    sql_results: List[SQLRefactorResult],
    json_output: bool = False,
) -> None:
    """Apply both YAML and SQL refactoring changes.

    Args:
        yaml_results: List of YAML refactoring results
        sql_results: List of SQL refactoring results
    """
    # Apply YAML changes
    for yaml_result in yaml_results:
        if yaml_result.refactored:
            yaml_result.update_yaml_file()
            yaml_result.print_to_console(json_output)

    # Apply SQL changes
    for sql_result in sql_results:
        if sql_result.refactored:
            sql_result.update_sql_file()
        sql_result.print_to_console(json_output)
