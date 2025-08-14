import yamllint.config
from typing import List, Dict, Any, Optional
from pathlib import Path

from dbt_autofix.refactors.results import YMLRuleRefactorResult, DbtDeprecationRefactor
from dbt_autofix.refactors.yml import DbtYAML
from dbt_autofix.retrieve_schemas import DbtProjectSpecs, SchemaSpecs

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)


def changeset_dbt_project_remove_deprecated_config(
    yml_str: str, exclude_dbt_project_keys: bool = False
) -> YMLRuleRefactorResult:
    """Remove deprecated keys"""
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    dict_deprecated_fields_with_defaults = {
        "log-path": "logs",
        "target-path": "target",
    }

    dict_renamed_fields = {
        "data-paths": "seed-paths",
        "source-paths": "model-paths",
    }

    dict_fields_to_deprecation_class = {
        "log-path": "ConfigLogPathDeprecation",
        "target-path": "ConfigTargetPathDeprecation",
        "data-paths": "ConfigDataPathDeprecation",
        "source-paths": "ConfigSourcePathDeprecation",
    }

    yml_dict = DbtYAML().load(yml_str) or {}

    for deprecated_field, _ in dict_deprecated_fields_with_defaults.items():
        if deprecated_field in yml_dict:
            if not exclude_dbt_project_keys:
                # by default we remove it
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed the deprecated field '{deprecated_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                del yml_dict[deprecated_field]
            # with the special field, we only remove it if it's different from the default
            elif yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log= f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                del yml_dict[deprecated_field]

    # TODO: add tests for this
    for deprecated_field, new_field in dict_renamed_fields.items():
        if deprecated_field in yml_dict:
            refactored = True
            if new_field not in yml_dict:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Renamed the deprecated field '{deprecated_field}' to '{new_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                yml_dict[new_field] = yml_dict[deprecated_field]
            else:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field]
                    )
                )
                yml_dict[new_field] = yml_dict[new_field] + yml_dict[deprecated_field]
            del yml_dict[deprecated_field]

    return YMLRuleRefactorResult(
        rule_name="remove_deprecated_config",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )

def rec_check_yaml_path(
    yml_dict: Dict[str, Any],
    path: Path,
    node_fields: DbtProjectSpecs,
    refactor_logs: Optional[List[str]] = None,
):
    # we can't set refactor_logs as an empty list

    # TODO: what about individual models in the config there?
    # indivdual models would show up here but without the `.sql` (or `.py`)

    if not path.exists() and not _path_exists_as_file(path):
        return yml_dict, [] if refactor_logs is None else refactor_logs

    yml_dict_copy = yml_dict.copy() if yml_dict else {}
    for k, v in yml_dict_copy.items():
        log_msg = None
        if not (path / k).exists() and not _path_exists_as_file(path / k):
            # Built-in config missing "+"
            if k in node_fields.allowed_config_fields_dbt_project:
                new_k = f"+{k}"
                yml_dict[new_k] = v
                log_msg = f"Added '+' in front of the nested config '{k}'"
            # Custom config not in meta
            elif not k.startswith("+"):
                log_msg = f"Moved custom config '{k}' to '+meta'"
                meta = yml_dict.get("+meta", {})
                meta.update({k: v})
                yml_dict["+meta"] = meta
            
            if log_msg:
                if refactor_logs is None:
                    refactor_logs = [log_msg]
                else:
                    refactor_logs.append(log_msg)
            
                del yml_dict[k]
        elif isinstance(yml_dict[k], dict):
            new_dict, refactor_logs = rec_check_yaml_path(yml_dict[k], path / k, node_fields, refactor_logs)
            yml_dict[k] = new_dict
    return yml_dict, [] if refactor_logs is None else refactor_logs

def _path_exists_as_file(path: Path) -> bool:
    return path.with_suffix(".py").exists() or path.with_suffix(".sql").exists() or path.with_suffix(".csv").exists()

def changeset_dbt_project_prefix_plus_for_config(
    yml_str: str, path: Path, schema_specs: SchemaSpecs
) -> YMLRuleRefactorResult:
    """Update keys for the config in dbt_project.yml under to prefix it with a `+`"""
    all_refactor_logs: List[str] = []

    yml_dict = DbtYAML().load(yml_str) or {}

    for node_type, node_fields in schema_specs.dbtproject_specs_per_node_type.items():
        for k, v in (yml_dict.get(node_type) or {}).copy().items():
            # check if this is the project name
            if k == yml_dict["name"]:
                new_dict, refactor_logs = rec_check_yaml_path(v, path / node_type, node_fields)
                yml_dict[node_type][k] = new_dict
                all_refactor_logs.extend(refactor_logs)

            # top level config
            elif k in node_fields.allowed_config_fields_dbt_project:
                all_refactor_logs.append(f"Added '+' in front of top level config '{k}'")
                new_k = f"+{k}"
                yml_dict[node_type][new_k] = v
                del yml_dict[node_type][k]

            # otherwise, treat it as a package
            # TODO: if this is not valid, we could delete it as well
            else:
                packages_path = path / Path(yml_dict.get("packages-paths", "dbt_packages"))
                new_dict, refactor_logs = rec_check_yaml_path(v, packages_path / k / node_type, node_fields)
                yml_dict[node_type][k] = new_dict
                all_refactor_logs.extend(refactor_logs)

    refactored = len(all_refactor_logs) > 0
    deprecation_refactors = [
        DbtDeprecationRefactor(
            log=log,
            deprecation="MissingPlusPrefixDeprecation"
        )
        for log in all_refactor_logs
    ]
    return YMLRuleRefactorResult(
        rule_name="prefix_plus_for_config",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_dbt_project_flip_behavior_flags(yml_str: str) -> YMLRuleRefactorResult:
    yml_dict = DbtYAML().load(yml_str) or {}
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactored = False

    behavior_change_flag_to_explainations = {
        "source_freshness_run_project_hooks": "run project hooks (on-run-start/on-run-end) as part of source freshness commands"
    }

    for key in yml_dict:
        if key == "flags":
            for behavior_change_flag in behavior_change_flag_to_explainations:
                if yml_dict["flags"].get(behavior_change_flag) is False:
                    yml_dict["flags"][behavior_change_flag] = True
                    refactored = True
                    deprecation_refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Set flag '{behavior_change_flag}' to 'True' - This will {behavior_change_flag_to_explainations[behavior_change_flag]}.",
                            deprecation="SourceFreshnessProjectHooksNotRun"
                        )
                    )

    return YMLRuleRefactorResult(
            rule_name="flip_behavior_flags",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )


def changeset_dbt_project_flip_test_arguments_behavior_flag(yml_str: str) -> YMLRuleRefactorResult:
    yml_dict = DbtYAML().load(yml_str) or {}
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactored = False

    existing_flags = yml_dict.get("flags", {})
    if existing_flags.get("require_generic_test_arguments_property") is False or "require_generic_test_arguments_property" not in existing_flags:
        yml_dict["flags"] = existing_flags
        yml_dict["flags"]["require_generic_test_arguments_property"] = True
        refactored = True
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log="Set flag 'require_generic_test_arguments_property' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
                deprecation="MissingGenericTestArgumentsPropertyDeprecation"
            )
        )

    return YMLRuleRefactorResult(
            rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )