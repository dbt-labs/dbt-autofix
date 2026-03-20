import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import yamllint.config
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.deprecations import ChangeType, DeprecationType
from dbt_autofix.refactors.node import Node, assign_node, extract_node, pop_node
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    DbtProjectYMLRefactorConfig,
    Location,
    RefactorEntry,
    YMLContent,
    YMLDeprecationRefactor,
    YMLRuleRefactorResult,
    find_key_at_path,
    location_of_key,
)
from dbt_autofix.refactors.yml import (
    CA_INLINE_IDX,
    DbtYAML,
    extract_preceding_text_comment,
    get_dict,
    load_yaml,
    rebalance_trailing_separator,
    set_blank_line_before,
)
from dbt_autofix.retrieve_schemas import DbtProjectSpecs, SchemaSpecs


@dataclass
class RefactorLog:
    message: str
    original_path: list = field(default_factory=list)
    edited_path: list = field(default_factory=list)

    def __contains__(self, item: str) -> bool:
        return item in self.message


config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)


def changeset_dbt_project_remove_deprecated_config(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Remove deprecated keys"""
    yml_str = content.current_str
    exclude_dbt_project_keys = config.exclude_dbt_project_keys
    refactored = False
    refactor_entries: List[RefactorEntry] = []

    dict_deprecated_fields_with_defaults = {
        "log-path": "logs",
        "target-path": "target",
    }

    dict_renamed_fields = {
        "data-paths": "seed-paths",
        "source-paths": "model-paths",
    }

    dict_fields_to_deprecation_class = {
        "log-path": DeprecationType.CONFIG_LOG_PATH_DEPRECATION,
        "target-path": DeprecationType.CONFIG_TARGET_PATH_DEPRECATION,
        "data-paths": DeprecationType.CONFIG_DATA_PATH_DEPRECATION,
        "source-paths": DeprecationType.CONFIG_SOURCE_PATH_DEPRECATION,
    }

    yml_dict = load_yaml(yml_str)

    for deprecated_field, _ in dict_deprecated_fields_with_defaults.items():
        if deprecated_field in yml_dict:
            if not exclude_dbt_project_keys:
                # by default we remove it
                refactored = True
                refactor_entries.append(
                    RefactorEntry(
                        refactor=DbtDeprecationRefactor(
                            log=f"Removed the deprecated field '{deprecated_field}'",
                            change_type=ChangeType.DEPRECATED_PROJECT_FIELD_REMOVED,
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                            original_location=location_of_key(content.original_parsed, deprecated_field),
                        )
                    )
                )
                del yml_dict[deprecated_field]
            # with the special field, we only remove it if it's different from the default
            elif yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                refactored = True
                refactor_entries.append(
                    RefactorEntry(
                        refactor=DbtDeprecationRefactor(
                            log=f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                            change_type=ChangeType.DEPRECATED_PROJECT_FIELD_REMOVED_NON_DEFAULT,
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                            original_location=location_of_key(content.original_parsed, deprecated_field),
                        )
                    )
                )
                del yml_dict[deprecated_field]

    # TODO: add tests for this
    for deprecated_field, new_field in dict_renamed_fields.items():
        if deprecated_field in yml_dict:
            refactored = True
            n = pop_node(yml_dict, deprecated_field, original_parent=content.original_parsed)
            if new_field not in yml_dict:
                refactor = DbtDeprecationRefactor(
                    log=f"Renamed the deprecated field '{deprecated_field}' to '{new_field}'",
                    change_type=ChangeType.DEPRECATED_PROJECT_FIELD_RENAMED,
                    deprecation=dict_fields_to_deprecation_class[deprecated_field],
                    original_location=n.original_location,
                )
            else:
                n.value = yml_dict[new_field] + n.value
                refactor = DbtDeprecationRefactor(
                    log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                    change_type=ChangeType.DEPRECATED_PROJECT_FIELD_MERGED,
                    deprecation=dict_fields_to_deprecation_class[deprecated_field],
                    original_location=n.original_location,
                )
            assign_node(yml_dict, new_field, n)

            refactor_entries.append(YMLDeprecationRefactor(refactor=refactor, edited_key_path=[new_field]).to_entry())

    refactored_yaml = DbtYAML().dump_to_string(yml_dict) if refactored else yml_str
    return YMLRuleRefactorResult(
        rule_name="remove_deprecated_config",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        refactor_entries=refactor_entries,
    )


def rec_check_yaml_path(
    parent: CommentedMap,
    key: Any,
    path: Path,
    node_fields: DbtProjectSpecs,
    refactor_logs: Optional[List[RefactorLog]] = None,
    schema_specs: Optional[SchemaSpecs] = None,
    node_type: Optional[str] = None,
    current_yaml_path: Optional[list] = None,
) -> tuple[Node, list[RefactorLog]]:
    # we can't set refactor_logs as an empty list

    # TODO: what about individual models in the config there?
    # indivdual models would show up here but without the `.sql` (or `.py`)

    if current_yaml_path is None:
        current_yaml_path = []

    n = extract_node(parent, key)
    yml_dict = n.value

    # Type guard: if value is not a CommentedMap, return it as-is
    # This handles cases where config values are lists, ints, strings, bools, etc.
    # For example: partition_by={'field': 'x', 'range': {...}}, cluster_by=['col1', 'col2']
    if not isinstance(yml_dict, CommentedMap):
        return n, [] if refactor_logs is None else refactor_logs

    original_keys = set(yml_dict.keys())
    for k, v in yml_dict.copy().items():
        log_msg = None
        original_path: list = []
        edited_path: list = []
        if not (path / k).exists() and not _path_exists_as_file(path / k):
            # Case 1: Key doesn't have "+" prefix
            if not k.startswith("+"):
                if k in node_fields.allowed_config_fields_dbt_project:
                    # Built-in config missing "+": rename in-place preserving position and comments
                    new_k = f"+{k}"
                    _pos = list(yml_dict.keys()).index(k)
                    n_sub = pop_node(yml_dict, k)
                    assign_node(yml_dict, new_k, n_sub, position=_pos)
                    log_msg = f"Added '+' in front of the nested config '{k}'"
                    original_path = [*current_yaml_path, k]
                    edited_path = [*current_yaml_path, new_k]
                elif isinstance(v, CommentedMap):
                    # Logical grouping (subdirectory-like structure): recurse
                    new_node, refactor_logs = rec_check_yaml_path(
                        yml_dict,
                        k,
                        path / k,
                        node_fields,
                        refactor_logs,
                        schema_specs,
                        node_type,
                        current_yaml_path=[*current_yaml_path, k],
                    )
                    assign_node(yml_dict, k, new_node)
                    rebalance_trailing_separator(yml_dict, k, original_keys)
                else:
                    # Custom leaf config: move to +meta
                    log_msg = f"Moved custom config '{k}' to '+meta'"
                    # A comment visually before k may be stored as the trailing token of
                    # the preceding key; extract it so it travels with k into +meta.
                    preceding_comment = extract_preceding_text_comment(yml_dict, k)
                    n_sub = pop_node(yml_dict, k)
                    if preceding_comment is not None:
                        if n_sub.comments is None:
                            n_sub.comments = [None, [preceding_comment], None, None]
                        elif len(n_sub.comments) > 1 and n_sub.comments[CA_INLINE_IDX] is None:
                            n_sub.comments[CA_INLINE_IDX] = [preceding_comment]
                    meta = get_dict(yml_dict, "+meta")
                    assign_node(meta, k, n_sub)
                    yml_dict["+meta"] = meta
                    original_path = [*current_yaml_path, k]
                    edited_path = [*current_yaml_path, "+meta", k]

                if log_msg:
                    entry = RefactorLog(log_msg, original_path, edited_path)
                    if refactor_logs is None:
                        refactor_logs = [entry]
                    else:
                        refactor_logs.append(entry)

            # Case 2: Key already has "+" prefix - validate it
            else:
                key_without_plus = k[1:]  # Remove the + prefix

                if key_without_plus in node_fields.allowed_config_fields_dbt_project:
                    # Valid config: check for invalid subkeys in dict-typed configs
                    if isinstance(v, CommentedMap) and schema_specs is not None:
                        dict_config_analysis = schema_specs.get_dict_config_analysis()
                        if key_without_plus in dict_config_analysis["specific_properties"]:
                            allowed_props = dict_config_analysis["specific_properties"][key_without_plus]
                            for subkey in v.copy():
                                if subkey.startswith("+"):
                                    # +prefixed subkey in a dict config - move to +meta
                                    n_sub = pop_node(v, subkey)
                                    meta = get_dict(yml_dict, "+meta")
                                    assign_node(meta, subkey, n_sub)
                                    yml_dict["+meta"] = meta
                                    entry = RefactorLog(
                                        f"Moved '{subkey}' from '{k}' to '+meta' (subkeys shouldn't be +prefixed)",
                                        [*current_yaml_path, k, subkey],
                                        [*current_yaml_path, "+meta", subkey],
                                    )
                                    if refactor_logs is None:
                                        refactor_logs = [entry]
                                    else:
                                        refactor_logs.append(entry)
                                elif subkey not in allowed_props:
                                    # Subkey not in allowed properties - move to +meta
                                    n_sub = pop_node(v, subkey)
                                    meta = get_dict(yml_dict, "+meta")
                                    assign_node(meta, subkey, n_sub)
                                    yml_dict["+meta"] = meta
                                    entry = RefactorLog(
                                        f"Moved '{subkey}' from '{k}' to '+meta' (not a valid property for {key_without_plus})",
                                        [*current_yaml_path, k, subkey],
                                        [*current_yaml_path, "+meta", subkey],
                                    )
                                    if refactor_logs is None:
                                        refactor_logs = [entry]
                                    else:
                                        refactor_logs.append(entry)

                else:
                    # Unrecognized +prefixed config: strip + and move to +meta
                    n_sub = pop_node(yml_dict, k)
                    meta = get_dict(yml_dict, "+meta")
                    assign_node(meta, key_without_plus, n_sub)
                    yml_dict["+meta"] = meta
                    entry = RefactorLog(
                        f"Moved unrecognized config '{k}' to '+meta'",
                        [*current_yaml_path, k],
                        [*current_yaml_path, "+meta", key_without_plus],
                    )
                    if refactor_logs is None:
                        refactor_logs = [entry]
                    else:
                        refactor_logs.append(entry)

        # Only recurse into CommentedMap values if the path exists (real directory/logical grouping)
        # Do NOT recurse into values of valid config keys (like +persist_docs, +labels)
        elif isinstance(yml_dict[k], CommentedMap):
            # Check if this is a valid config key - if so, its value is the config value, not nested configs
            is_valid_config = k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
            if not is_valid_config:
                new_node, refactor_logs = rec_check_yaml_path(
                    yml_dict,
                    k,
                    path / k,
                    node_fields,
                    refactor_logs,
                    schema_specs,
                    node_type,
                    current_yaml_path=[*current_yaml_path, k],
                )
                assign_node(yml_dict, k, new_node)
                rebalance_trailing_separator(yml_dict, k, original_keys)
    return n, [] if refactor_logs is None else refactor_logs


def _path_exists_as_file(path: Path) -> bool:
    return path.with_suffix(".py").exists() or path.with_suffix(".sql").exists() or path.with_suffix(".csv").exists()


def changeset_dbt_project_prefix_plus_for_config(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Update keys for the config in dbt_project.yml under to prefix it with a `+`"""
    yml_str = content.current_str
    path = config.root_path
    schema_specs = config.schema_specs
    all_refactor_logs: List[RefactorLog] = []

    yml_dict = load_yaml(yml_str)

    for node_type, node_fields in schema_specs.dbtproject_specs_per_node_type.items():
        for k, v in get_dict(yml_dict, node_type).copy().items():
            # check if this is the project name
            if k == yml_dict["name"]:
                # Only recurse if v is a CommentedMap (should be project configs)
                if isinstance(v, CommentedMap):
                    new_node, refactor_logs = rec_check_yaml_path(
                        yml_dict[node_type],
                        k,
                        path / node_type,
                        node_fields,
                        None,
                        schema_specs,
                        node_type,
                        current_yaml_path=[node_type, k],
                    )
                    assign_node(yml_dict[node_type], k, new_node)
                    all_refactor_logs.extend(refactor_logs)
                # else: non-CommentedMap value, keep as-is (unusual but possible)

            # top level config (with or without + prefix)
            elif k in node_fields.allowed_config_fields_dbt_project or (
                k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
            ):
                # Config key is valid - if it doesn't have +, add it
                if not k.startswith("+"):
                    new_k = f"+{k}"
                    all_refactor_logs.append(
                        RefactorLog(
                            f"Added '+' in front of top level config '{k}'",
                            [node_type, k],
                            [node_type, new_k],
                        )
                    )
                    _keys = list(yml_dict[node_type].keys())
                    _pos = _keys.index(k)
                    n = pop_node(yml_dict[node_type], k)
                    assign_node(yml_dict[node_type], new_k, n, position=_pos)
                # else: already has +, keep as-is, value is the config value (don't recurse)

            # otherwise, treat it as a package or logical grouping
            # TODO: if this is not valid, we could delete it as well
            else:
                packages_path = path / Path(yml_dict.get("packages-paths", "dbt_packages"))
                # Only recurse if v is a CommentedMap (should be package configs or logical grouping)
                if isinstance(v, CommentedMap):
                    new_node, refactor_logs = rec_check_yaml_path(
                        yml_dict[node_type],
                        k,
                        packages_path / k / node_type,
                        node_fields,
                        None,
                        schema_specs,
                        node_type,
                        current_yaml_path=[node_type, k],
                    )
                    assign_node(yml_dict[node_type], k, new_node)
                    all_refactor_logs.extend(refactor_logs)
                # else: non-CommentedMap value, keep as-is (unusual but possible)

    refactored = len(all_refactor_logs) > 0
    refactored_yaml = DbtYAML().dump_to_string(yml_dict) if refactored else yml_str
    refactor_entries: List[RefactorEntry] = []
    for log in all_refactor_logs:
        refactor = DbtDeprecationRefactor(
            log=log.message,
            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
            original_location=find_key_at_path(content.original_parsed, log.original_path)
            if log.original_path
            else None,
        )
        refactor_entries.append(YMLDeprecationRefactor(refactor=refactor, edited_key_path=log.edited_path).to_entry())
    return YMLRuleRefactorResult(
        rule_name="prefix_plus_for_config",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        refactor_entries=refactor_entries,
    )


def changeset_dbt_project_flip_behavior_flags(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactored = False

    behavior_change_flag_to_explainations = {
        "source_freshness_run_project_hooks": "run project hooks (on-run-start/on-run-end) as part of source freshness commands"
    }

    flags_flipped: List[str] = []
    for key in yml_dict:
        if key == "flags":
            for behavior_change_flag in behavior_change_flag_to_explainations:
                if yml_dict["flags"].get(behavior_change_flag) is False:
                    yml_dict["flags"][behavior_change_flag] = True
                    refactored = True
                    flags_flipped.append(behavior_change_flag)

    refactored_yaml = DbtYAML().dump_to_string(yml_dict) if refactored else yml_str
    original_flags = get_dict(content.original_parsed, "flags")
    refactor_entries: List[RefactorEntry] = []
    for flag in flags_flipped:
        refactor = DbtDeprecationRefactor(
            log=f"Set flag '{flag}' to 'True' - This will {behavior_change_flag_to_explainations[flag]}.",
            deprecation=DeprecationType.SOURCE_FRESHNESS_PROJECT_HOOKS_NOT_RUN,
            change_type=ChangeType.SOURCE_FRESHNESS_PROJECT_HOOKS_NOT_RUN,
            original_location=location_of_key(original_flags, flag),
        )
        refactor_entries.append(YMLDeprecationRefactor(refactor=refactor, edited_key_path=["flags", flag]).to_entry())

    return YMLRuleRefactorResult(
        rule_name="flip_behavior_flags",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        refactor_entries=refactor_entries,
    )


def changeset_dbt_project_flip_test_arguments_behavior_flag(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)
    refactor_entries: List[RefactorEntry] = []
    refactored = False

    _flag = "require_generic_test_arguments_property"
    existing_flags = get_dict(yml_dict, "flags")
    flag_existed = _flag in existing_flags
    if existing_flags.get(_flag) is False or not flag_existed:
        yml_dict["flags"] = existing_flags
        yml_dict["flags"][_flag] = True
        set_blank_line_before(yml_dict, "flags")
        refactored = True
        refactored_yaml = DbtYAML().dump_to_string(yml_dict)
        original_flags = get_dict(content.original_parsed, "flags")
        refactor = DbtDeprecationRefactor(
            log=f"Set flag '{_flag}' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
            change_type=ChangeType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION_FIX,
            deprecation=DeprecationType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION,
            original_location=location_of_key(original_flags, _flag) if flag_existed else None,
        )
        refactor_entries.append(YMLDeprecationRefactor(refactor=refactor, edited_key_path=["flags", _flag]).to_entry())
    else:
        refactored_yaml = yml_str

    return YMLRuleRefactorResult(
        rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        refactor_entries=refactor_entries,
    )


def changeset_fix_space_after_plus(content: YMLContent, config: DbtProjectYMLRefactorConfig) -> YMLRuleRefactorResult:
    """Fix keys that have a space after the '+' prefix (e.g., '+ tags' -> '+tags').

    This fixes the dbt1060 error: "Ignored unexpected key '+ tags'".
    When users accidentally add a space after the '+' in config keys, it creates
    an invalid key. This function:
    - Fixes valid keys by removing the space (e.g., '+ tags:' -> '+tags:')
    - Removes invalid keys entirely (keys not in the schema), including their values
    """
    yml_str = content.current_str
    schema_specs = config.schema_specs
    refactored = False
    refactor_entries: List[RefactorEntry] = []

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
            refactor_entries=[],
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
            refactor_entries.insert(
                0,
                RefactorEntry(
                    refactor=DbtDeprecationRefactor(
                        log=f"Removed space after '+' in key '+ {key_name}' on line {line_num}, changed to '{corrected_key}'",
                        change_type=ChangeType.SPACE_AFTER_PLUS_FIXUP,
                        original_location=Location(line=line_num),
                        edited_location=Location(line=line_num),
                    )
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
            refactor_entries.insert(
                0,
                RefactorEntry(
                    refactor=DbtDeprecationRefactor(
                        log=f"Removed invalid key '+ {key_name}' on line {line_num} (not a valid config key)",
                        change_type=ChangeType.INVALID_KEY_AFTER_PLUS_REMOVED,
                        original_location=Location(line=line_num),
                    )
                ),
            )

    return YMLRuleRefactorResult(
        rule_name="fix_space_after_plus",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        refactor_entries=refactor_entries,
    )
