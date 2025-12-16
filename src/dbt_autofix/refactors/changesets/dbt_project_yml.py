import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yamllint.config

from dbt_autofix.refactors.results import DbtDeprecationRefactor, YMLRuleRefactorResult
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
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
                    )
                )
                del yml_dict[deprecated_field]
            # with the special field, we only remove it if it's different from the default
            elif yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
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
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
                    )
                )
                yml_dict[new_field] = yml_dict[deprecated_field]
            else:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
                    )
                )
                yml_dict[new_field] = yml_dict[new_field] + yml_dict[deprecated_field]
            del yml_dict[deprecated_field]

    return YMLRuleRefactorResult(
        rule_name="remove_deprecated_config",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def rec_check_yaml_path(
    yml_dict: Any,
    path: Path,
    node_fields: DbtProjectSpecs,
    refactor_logs: Optional[List[str]] = None,
    schema_specs: Optional[SchemaSpecs] = None,
    node_type: Optional[str] = None,
):
    # we can't set refactor_logs as an empty list

    # TODO: what about individual models in the config there?
    # indivdual models would show up here but without the `.sql` (or `.py`)

    # Don't early return if path doesn't exist - we still need to process
    # logical groupings (YAML structure that doesn't correspond to directories)
    # The per-key check below (line 115) handles the actual file/dir validation

    # Type guard: if yml_dict is not a dict, return it as-is
    # This handles cases where config values are lists, ints, strings, bools, etc.
    # For example: partition_by={'field': 'x', 'range': {...}}, cluster_by=['col1', 'col2']
    if not isinstance(yml_dict, dict):
        return yml_dict, [] if refactor_logs is None else refactor_logs

    yml_dict_copy = yml_dict.copy() if yml_dict else {}
    for k, v in yml_dict_copy.items():
        log_msg = None
        if not (path / k).exists() and not _path_exists_as_file(path / k):
            # Case 1: Key doesn't have "+" prefix
            if not k.startswith("+"):
                # Built-in config missing "+"
                if k in node_fields.allowed_config_fields_dbt_project:
                    new_k = f"+{k}"
                    yml_dict[new_k] = v
                    log_msg = f"Added '+' in front of the nested config '{k}'"
                # Check if this is a dict value (logical grouping)
                # Only recurse if it's NOT a valid config key
                elif isinstance(v, dict):
                    # This is a logical grouping (subdirectory-like structure in YAML)
                    # Recurse into it to process nested configs
                    new_dict, refactor_logs = rec_check_yaml_path(
                        v, path / k, node_fields, refactor_logs, schema_specs, node_type
                    )
                    yml_dict[k] = new_dict
                # Custom config not in meta (leaf value)
                else:
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

            # Case 2: Key already has "+" prefix - validate it
            else:
                key_without_plus = k[1:]  # Remove the + prefix

                # Check if it's a valid config field
                if key_without_plus in node_fields.allowed_config_fields_dbt_project:
                    # Valid config, but we need to check if it's a dict with +prefixed subkeys
                    if isinstance(v, dict) and schema_specs is not None:
                        # Get dict config analysis
                        dict_config_analysis = schema_specs.get_dict_config_analysis()

                        # Check if this config has specific properties (not open-ended)
                        if key_without_plus in dict_config_analysis["specific_properties"]:
                            # This config has specific allowed properties
                            allowed_props = dict_config_analysis["specific_properties"][key_without_plus]
                            dict_copy = v.copy()

                            for subkey, subvalue in dict_copy.items():
                                # Check if subkey has + prefix when it shouldn't
                                if subkey.startswith("+"):
                                    # +prefixed subkey in a dict config - move to +meta
                                    log_msg = f"Moved '{subkey}' from '{k}' to '+meta' (subkeys shouldn't be +prefixed)"
                                    meta = yml_dict.get("+meta", {})
                                    meta[subkey] = subvalue
                                    yml_dict["+meta"] = meta
                                    del v[subkey]

                                    if refactor_logs is None:
                                        refactor_logs = [log_msg]
                                    else:
                                        refactor_logs.append(log_msg)
                                # Check if subkey without + is not in allowed properties
                                elif subkey not in allowed_props:
                                    # Subkey not in allowed properties - move to +meta
                                    log_msg = f"Moved '{subkey}' from '{k}' to '+meta' (not a valid property for {key_without_plus})"
                                    meta = yml_dict.get("+meta", {})
                                    meta[subkey] = subvalue
                                    yml_dict["+meta"] = meta
                                    del v[subkey]

                                    if refactor_logs is None:
                                        refactor_logs = [log_msg]
                                    else:
                                        refactor_logs.append(log_msg)
                    # Otherwise keep as-is (value is the config value)

                # Unrecognized config (not in schema), move to +meta
                else:
                    log_msg = f"Moved unrecognized config '{k}' to '+meta'"
                    meta = yml_dict.get("+meta", {})
                    meta.update({key_without_plus: v})
                    yml_dict["+meta"] = meta
                    del yml_dict[k]

                    if refactor_logs is None:
                        refactor_logs = [log_msg]
                    else:
                        refactor_logs.append(log_msg)

        # Only recurse into dict values if the path exists (real directory/logical grouping)
        # Do NOT recurse into values of valid config keys (like +persist_docs, +labels)
        elif isinstance(yml_dict[k], dict):
            # Check if this is a valid config key - if so, its value is the config value, not nested configs
            is_valid_config = k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
            if not is_valid_config:
                new_dict, refactor_logs = rec_check_yaml_path(
                    yml_dict[k], path / k, node_fields, refactor_logs, schema_specs, node_type
                )
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
                # Only recurse if v is a dict (should be project configs)
                if isinstance(v, dict):
                    new_dict, refactor_logs = rec_check_yaml_path(
                        v, path / node_type, node_fields, None, schema_specs, node_type
                    )
                    yml_dict[node_type][k] = new_dict
                    all_refactor_logs.extend(refactor_logs)
                # else: non-dict value, keep as-is (unusual but possible)

            # top level config (with or without + prefix)
            elif k in node_fields.allowed_config_fields_dbt_project or (
                k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
            ):
                # Config key is valid - if it doesn't have +, add it
                if not k.startswith("+"):
                    all_refactor_logs.append(f"Added '+' in front of top level config '{k}'")
                    new_k = f"+{k}"
                    yml_dict[node_type][new_k] = v
                    del yml_dict[node_type][k]
                # else: already has +, keep as-is, value is the config value (don't recurse)

            # otherwise, treat it as a package or logical grouping
            # TODO: if this is not valid, we could delete it as well
            else:
                packages_path = path / Path(yml_dict.get("packages-paths", "dbt_packages"))
                # Only recurse if v is a dict (should be package configs or logical grouping)
                if isinstance(v, dict):
                    new_dict, refactor_logs = rec_check_yaml_path(
                        v, packages_path / k / node_type, node_fields, None, schema_specs, node_type
                    )
                    yml_dict[node_type][k] = new_dict
                    all_refactor_logs.extend(refactor_logs)
                # else: non-dict value, keep as-is (unusual but possible)

    refactored = len(all_refactor_logs) > 0
    deprecation_refactors = [
        DbtDeprecationRefactor(log=log, deprecation="MissingPlusPrefixDeprecation") for log in all_refactor_logs
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
                            deprecation="SourceFreshnessProjectHooksNotRun",
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
    if (
        existing_flags.get("require_generic_test_arguments_property") is False
        or "require_generic_test_arguments_property" not in existing_flags
    ):
        yml_dict["flags"] = existing_flags
        yml_dict["flags"]["require_generic_test_arguments_property"] = True
        refactored = True
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log="Set flag 'require_generic_test_arguments_property' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
                deprecation="MissingGenericTestArgumentsPropertyDeprecation",
            )
        )

    return YMLRuleRefactorResult(
        rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,  # type: ignore
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_fix_space_after_plus(yml_str: str, schema_specs: SchemaSpecs) -> YMLRuleRefactorResult:
    """Fix keys that have a space after the '+' prefix (e.g., '+ tags' -> '+tags').

    This fixes the dbt1060 error: "Ignored unexpected key '+ tags'".
    When users accidentally add a space after the '+' in config keys, it creates
    an invalid key. This function:
    - Fixes valid keys by removing the space (e.g., '+ tags:' -> '+tags:')
    - Removes invalid keys entirely (keys not in the schema), including their values

    Args:
        yml_str: The YAML string to process
        schema_specs: The schema specifications to validate corrected keys against

    Returns:
        YMLRuleRefactorResult containing the refactored YAML and any changes made
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Pattern to match keys with space after plus: "+ key:" at the start of the line (after indentation)
    pattern = re.compile(r"^(\s*)\+\s+(\w+)(\s*:)", re.MULTILINE)

    # First, let's identify all the matches
    matches = list(pattern.finditer(yml_str))

    if not matches:
        return YMLRuleRefactorResult(
            rule_name="fix_space_after_plus",
            refactored=False,
            refactored_yaml=yml_str,
            original_yaml=yml_str,
            deprecation_refactors=[],
        )

    # Collect all valid config keys from schema specs (with + prefix)
    all_valid_config_keys = set()
    for node_type, node_fields in schema_specs.dbtproject_specs_per_node_type.items():
        all_valid_config_keys.update(node_fields.allowed_config_fields_dbt_project_with_plus)

    # Separate matches into valid (fix) and invalid (remove) keys
    # Process in reverse order to maintain correct offsets when removing/replacing
    matches_with_action = []
    for match in matches:
        key_name = match.group(2)
        corrected_key = f"+{key_name}"
        line_num = yml_str[: match.start()].count("\n") + 1

        if corrected_key in all_valid_config_keys:
            # Valid key - fix by removing space
            matches_with_action.append(("fix", match, corrected_key, key_name, line_num))
        else:
            # Invalid key - remove entire entry
            matches_with_action.append(("remove", match, corrected_key, key_name, line_num))

    # Sort by position in reverse to process from end to start (to maintain positions)
    matches_with_action.sort(key=lambda x: x[1].start(), reverse=True)

    # Build the refactored string
    refactored_yaml = yml_str

    for action, match, corrected_key, key_name, line_num in matches_with_action:
        if action == "fix":
            # Fix by removing space
            indent = match.group(1)
            colon_and_space = match.group(3)
            original_full_match = match.group(0)
            corrected_full = f"{indent}{corrected_key}{colon_and_space}"

            start_pos = match.start()
            end_pos = match.end()

            refactored_yaml = refactored_yaml[:start_pos] + corrected_full + refactored_yaml[end_pos:]

            refactored = True
            deprecation_refactors.insert(
                0,
                DbtDeprecationRefactor(
                    log=f"Removed space after '+' in key '+ {key_name}' on line {line_num}, changed to '{corrected_key}'"
                ),
            )
        else:  # action == 'remove'
            # Remove the entire key-value entry
            # We need to find the entire block to remove, including nested content
            start_line_pos = refactored_yaml.rfind("\n", 0, match.start()) + 1
            indent = match.group(1)

            # Find the end of this entry by looking for the next line with same or less indentation
            # or the next key at same level
            lines = refactored_yaml[start_line_pos:].split("\n")
            lines_to_remove = 1  # Start with the key line itself

            # Check subsequent lines
            for i in range(1, len(lines)):
                line = lines[i]
                if line.strip() == "":
                    # Empty line - include it
                    lines_to_remove += 1
                    continue

                # Calculate indentation
                line_indent = len(line) - len(line.lstrip())
                key_indent = len(indent)

                # If this line has more indentation, it's part of the value
                if line_indent > key_indent:
                    lines_to_remove += 1
                else:
                    # Same or less indentation - this is the next entry
                    break

            # Calculate the end position
            lines_text = "\n".join(lines[:lines_to_remove])
            end_pos = start_line_pos + len(lines_text)
            if end_pos < len(refactored_yaml) and refactored_yaml[end_pos] == "\n":
                end_pos += 1  # Include the trailing newline

            # Remove the block
            refactored_yaml = refactored_yaml[:start_line_pos] + refactored_yaml[end_pos:]

            refactored = True
            deprecation_refactors.insert(
                0,
                DbtDeprecationRefactor(
                    log=f"Removed invalid key '+ {key_name}' on line {line_num} (not a valid config key)"
                ),
            )

    return YMLRuleRefactorResult(
        rule_name="fix_space_after_plus",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )
