import re
from pathlib import Path
from typing import Any, Optional

import yamllint.config
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.deprecations import ChangeType, DeprecationType
from dbt_autofix.refactors.node import Node, assign_node, extract_node, pop_node, reattach_next_key_above_comment
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    DbtProjectYMLRefactorConfig,
    Location,
    RefactorEntry,
    YMLContent,
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
)
from dbt_autofix.retrieve_schemas import DbtProjectSpecs


config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)


def changeset_dbt_project_remove_deprecated_config(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Remove deprecated keys"""
    return _RemoveDeprecatedConfigImpl(content, config).execute()


class _RemoveDeprecatedConfigImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.exclude_dbt_project_keys = config.exclude_dbt_project_keys
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: list[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_deprecated_config",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
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

        for deprecated_field, _ in dict_deprecated_fields_with_defaults.items():
            if deprecated_field in self.yml_dict:
                if not self.exclude_dbt_project_keys:
                    # by default we remove it
                    self._refactored = True
                    self._refactor_entries.append(
                        RefactorEntry(
                            refactor=DbtDeprecationRefactor(
                                log=f"Removed the deprecated field '{deprecated_field}'",
                                change_type=ChangeType.DEPRECATED_PROJECT_FIELD_REMOVED,
                                deprecation=dict_fields_to_deprecation_class[deprecated_field],
                                original_location=location_of_key(self.content.original_parsed, deprecated_field),
                            )
                        )
                    )
                    del self.yml_dict[deprecated_field]
                # with the special field, we only remove it if it's different from the default
                elif self.yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                    self._refactored = True
                    self._refactor_entries.append(
                        RefactorEntry(
                            refactor=DbtDeprecationRefactor(
                                log=f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                                change_type=ChangeType.DEPRECATED_PROJECT_FIELD_REMOVED_NON_DEFAULT,
                                deprecation=dict_fields_to_deprecation_class[deprecated_field],
                                original_location=location_of_key(self.content.original_parsed, deprecated_field),
                            )
                        )
                    )
                    del self.yml_dict[deprecated_field]

        # TODO: add tests for this
        for deprecated_field, new_field in dict_renamed_fields.items():
            if deprecated_field in self.yml_dict:
                self._refactored = True
                if new_field not in self.yml_dict:
                    r = DbtDeprecationRefactor(
                        log=f"Renamed the deprecated field '{deprecated_field}' to '{new_field}'",
                        change_type=ChangeType.DEPRECATED_PROJECT_FIELD_RENAMED,
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        original_location=location_of_key(self.content.original_parsed, deprecated_field),
                    )
                    self.yml_dict[new_field] = self.yml_dict[deprecated_field]
                else:
                    r = DbtDeprecationRefactor(
                        log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                        change_type=ChangeType.DEPRECATED_PROJECT_FIELD_MERGED,
                        deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        original_location=location_of_key(self.content.original_parsed, deprecated_field),
                    )
                    self.yml_dict[new_field] = self.yml_dict[new_field] + self.yml_dict[deprecated_field]
                del self.yml_dict[deprecated_field]

                def resolve(parsed, refactor=r, field=new_field):
                    refactor.edited_location = location_of_key(parsed, field)

                self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))


def _path_exists_as_file(path: Path) -> bool:
    return path.with_suffix(".py").exists() or path.with_suffix(".sql").exists() or path.with_suffix(".csv").exists()


def changeset_dbt_project_prefix_plus_for_config(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Update keys for the config in dbt_project.yml under to prefix it with a `+`"""
    return _PrefixPlusForConfigImpl(content, config).execute()


class _PrefixPlusForConfigImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.root_path = config.root_path
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: list[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="prefix_plus_for_config",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        for node_type, node_fields in self.schema_specs.dbtproject_specs_per_node_type.items():
            for k, v in get_dict(self.yml_dict, node_type).copy().items():
                node_dict = get_dict(self.yml_dict, node_type)
                # check if this is the project name
                if k == self.yml_dict["name"]:
                    # Only recurse if v is a CommentedMap (should be project configs)
                    if isinstance(v, CommentedMap):
                        new_node = self._rec_check_yaml_path(
                            node_dict,
                            k,
                            self.root_path / node_type,
                            node_fields,
                            node_type,
                            current_yaml_path=[node_type, k],
                        )
                        assign_node(node_dict, k, new_node)
                    # else: non-dict value, keep as-is (unusual but possible)

                # top level config (with or without + prefix)
                elif k in node_fields.allowed_config_fields_dbt_project or (
                    k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
                ):
                    # Config key is valid - if it doesn't have +, add it
                    if not k.startswith("+"):
                        new_k = f"+{k}"
                        refactor = DbtDeprecationRefactor(
                            log=f"Added '+' in front of top level config '{k}'",
                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                            original_location=find_key_at_path(self.content.original_parsed, [node_type, k]),
                        )
                        self._refactored = True

                        def resolve(parsed, refactor=refactor, new_k=new_k, node_type=node_type):
                            refactor.edited_location = find_key_at_path(parsed, [node_type, new_k])

                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))
                        self.yml_dict[node_type][new_k] = v
                        del self.yml_dict[node_type][k]
                    # else: already has +, keep as-is, value is the config value (don't recurse)

                # otherwise, treat it as a package or logical grouping
                # TODO: if this is not valid, we could delete it as well
                else:
                    packages_path = self.root_path / Path(self.yml_dict.get("packages-paths", "dbt_packages"))
                    # Only recurse if v is a CommentedMap (should be package configs or logical grouping)
                    if isinstance(v, CommentedMap):
                        new_node = self._rec_check_yaml_path(
                            node_dict,
                            k,
                            packages_path / k / node_type,
                            node_fields,
                            node_type,
                            current_yaml_path=[node_type, k],
                        )
                        assign_node(node_dict, k, new_node)
                    # else: non-dict value, keep as-is (unusual but possible)

    def _rec_check_yaml_path(
        self,
        parent: CommentedMap,
        key: Any,
        path: Path,
        node_fields: DbtProjectSpecs,
        node_type: Optional[str] = None,
        current_yaml_path: Optional[list] = None,
    ) -> Node:
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
            return n

        original_keys = set(yml_dict.keys())
        for k, v in yml_dict.copy().items():
            if not (path / k).exists() and not _path_exists_as_file(path / k):
                # Case 1: Key doesn't have "+" prefix
                if not k.startswith("+"):
                    if k in node_fields.allowed_config_fields_dbt_project:
                        # Built-in config missing "+": rename in-place preserving position and comments
                        new_k = f"+{k}"
                        _pos = list(yml_dict.keys()).index(k)
                        n_sub = pop_node(yml_dict, k)
                        assign_node(yml_dict, new_k, n_sub, position=_pos)
                        refactor = DbtDeprecationRefactor(
                            log=f"Added '+' in front of the nested config '{k}'",
                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                            original_location=find_key_at_path(
                                self.content.original_parsed, [*current_yaml_path, k]
                            ),
                        )
                        self._refactored = True

                        def resolve(parsed, refactor=refactor, cp=current_yaml_path, new_k=new_k):
                            refactor.edited_location = find_key_at_path(parsed, [*cp, new_k])

                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))
                    elif isinstance(v, CommentedMap):
                        # Logical grouping (subdirectory-like structure): recurse
                        new_node = self._rec_check_yaml_path(
                            yml_dict, k, path / k, node_fields, node_type, current_yaml_path=[*current_yaml_path, k]
                        )
                        assign_node(yml_dict, k, new_node)
                        rebalance_trailing_separator(yml_dict, k, original_keys)
                    else:
                        # Custom leaf config: move to +meta
                        preceding_comment = extract_preceding_text_comment(yml_dict, k)
                        _keys = list(yml_dict.keys())
                        _k_idx = _keys.index(k)
                        _next_key = _keys[_k_idx + 1] if _k_idx + 1 < len(_keys) else None
                        n_sub = pop_node(yml_dict, k)
                        if preceding_comment is not None:
                            if n_sub.comments is None:
                                n_sub.comments = [None, [preceding_comment], None, None]
                            elif len(n_sub.comments) > 1 and n_sub.comments[CA_INLINE_IDX] is None:
                                n_sub.comments[CA_INLINE_IDX] = [preceding_comment]
                        reattach_next_key_above_comment(n_sub, yml_dict, _next_key)
                        meta = get_dict(yml_dict, "+meta")
                        assign_node(meta, k, n_sub)
                        yml_dict["+meta"] = meta
                        refactor = DbtDeprecationRefactor(
                            log=f"Moved custom config '{k}' to '+meta'",
                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                            original_location=find_key_at_path(
                                self.content.original_parsed, [*current_yaml_path, k]
                            ),
                        )
                        self._refactored = True

                        def resolve(parsed, refactor=refactor, cp=current_yaml_path):
                            refactor.edited_location = find_key_at_path(parsed, [*cp, "+meta"])

                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))

                # Case 2: Key already has "+" prefix - validate it
                else:
                    key_without_plus = k[1:]

                    if key_without_plus in node_fields.allowed_config_fields_dbt_project:
                        # Valid config: check for invalid subkeys in dict-typed configs
                        if isinstance(v, CommentedMap) and self.schema_specs is not None:
                            dict_config_analysis = self.schema_specs.get_dict_config_analysis()
                            if key_without_plus in dict_config_analysis["specific_properties"]:
                                allowed_props = dict_config_analysis["specific_properties"][key_without_plus]
                                for subkey in v.copy():
                                    if subkey.startswith("+"):
                                        n_sub = pop_node(v, subkey)
                                        meta = get_dict(yml_dict, "+meta")
                                        assign_node(meta, subkey, n_sub)
                                        yml_dict["+meta"] = meta
                                        refactor = DbtDeprecationRefactor(
                                            log=f"Moved '{subkey}' from '{k}' to '+meta' (subkeys shouldn't be +prefixed)",
                                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                                            original_location=find_key_at_path(
                                                self.content.original_parsed, [*current_yaml_path, k, subkey]
                                            ),
                                        )
                                        self._refactored = True

                                        def resolve(parsed, refactor=refactor, cp=current_yaml_path, sk=subkey):
                                            refactor.edited_location = find_key_at_path(
                                                parsed, [*cp, "+meta", sk]
                                            )

                                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))
                                    elif subkey not in allowed_props:
                                        n_sub = pop_node(v, subkey)
                                        meta = get_dict(yml_dict, "+meta")
                                        assign_node(meta, subkey, n_sub)
                                        yml_dict["+meta"] = meta
                                        refactor = DbtDeprecationRefactor(
                                            log=f"Moved '{subkey}' from '{k}' to '+meta' (not a valid property for {key_without_plus})",
                                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                                            original_location=find_key_at_path(
                                                self.content.original_parsed, [*current_yaml_path, k, subkey]
                                            ),
                                        )
                                        self._refactored = True

                                        def resolve(parsed, refactor=refactor, cp=current_yaml_path, sk=subkey):
                                            refactor.edited_location = find_key_at_path(
                                                parsed, [*cp, "+meta", sk]
                                            )

                                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))

                    else:
                        # Unrecognized +prefixed config: strip + and move to +meta
                        n_sub = pop_node(yml_dict, k)
                        meta = get_dict(yml_dict, "+meta")
                        assign_node(meta, key_without_plus, n_sub)
                        yml_dict["+meta"] = meta
                        refactor = DbtDeprecationRefactor(
                            log=f"Moved unrecognized config '{k}' to '+meta'",
                            change_type=ChangeType.MISSING_PLUS_PREFIX_DEPRECATION_FIX,
                            deprecation=DeprecationType.MISSING_PLUS_PREFIX_DEPRECATION,
                            original_location=find_key_at_path(
                                self.content.original_parsed, [*current_yaml_path, k]
                            ),
                        )
                        self._refactored = True

                        def resolve(parsed, refactor=refactor, cp=current_yaml_path, kwp=key_without_plus):
                            refactor.edited_location = find_key_at_path(parsed, [*cp, "+meta", kwp])

                        self._refactor_entries.append(RefactorEntry(refactor=refactor, resolve=resolve))

            # Only recurse into CommentedMap values if the path exists (real directory/logical grouping)
            # Do NOT recurse into values of valid config keys (like +persist_docs, +labels)
            elif isinstance(yml_dict[k], CommentedMap):
                is_valid_config = k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
                if not is_valid_config:
                    new_node = self._rec_check_yaml_path(
                        yml_dict, k, path / k, node_fields, node_type, current_yaml_path=[*current_yaml_path, k]
                    )
                    assign_node(yml_dict, k, new_node)
                    rebalance_trailing_separator(yml_dict, k, original_keys)
        return n


def changeset_dbt_project_flip_behavior_flags(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _FlipBehaviorFlagsImpl(content, config).execute()


class _FlipBehaviorFlagsImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: list[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="flip_behavior_flags",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        behavior_change_flag_to_explainations = {
            "source_freshness_run_project_hooks": "run project hooks (on-run-start/on-run-end) as part of source freshness commands"
        }

        original_flags = self.content.original_parsed.get("flags", {})
        for key in self.yml_dict:
            if key == "flags":
                for behavior_change_flag in behavior_change_flag_to_explainations:
                    if self.yml_dict["flags"].get(behavior_change_flag) is False:
                        self.yml_dict["flags"][behavior_change_flag] = True
                        self._refactored = True
                        r = DbtDeprecationRefactor(
                            log=f"Set flag '{behavior_change_flag}' to 'True' - This will {behavior_change_flag_to_explainations[behavior_change_flag]}.",
                            change_type=ChangeType.SOURCE_FRESHNESS_PROJECT_HOOKS_NOT_RUN,
                            deprecation=DeprecationType.SOURCE_FRESHNESS_PROJECT_HOOKS_NOT_RUN,
                            original_location=location_of_key(original_flags, behavior_change_flag),
                        )

                        def resolve(parsed, refactor=r, flag=behavior_change_flag):
                            refactor.edited_location = location_of_key(get_dict(parsed, "flags"), flag)

                        self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))


def changeset_dbt_project_flip_test_arguments_behavior_flag(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _FlipTestArgumentsBehaviorFlagImpl(content, config).execute()


class _FlipTestArgumentsBehaviorFlagImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: list[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        _flag = "require_generic_test_arguments_property"
        existing_flags = get_dict(self.yml_dict, "flags")
        flag_existed = _flag in existing_flags
        if existing_flags.get(_flag) is False or not flag_existed:
            self.yml_dict["flags"] = existing_flags
            self.yml_dict["flags"][_flag] = True
            self._refactored = True
            original_flags = self.content.original_parsed.get("flags", {})
            r = DbtDeprecationRefactor(
                log=f"Set flag '{_flag}' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
                change_type=ChangeType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION_FIX,
                deprecation=DeprecationType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION,
                original_location=location_of_key(original_flags, _flag) if flag_existed else None,
            )

            def resolve(parsed, refactor=r):
                refactor.edited_location = location_of_key(get_dict(parsed, "flags"), _flag)

            self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))


def changeset_fix_space_after_plus(content: YMLContent, config: DbtProjectYMLRefactorConfig) -> YMLRuleRefactorResult:
    r"""Fix keys that have a space after the '+' prefix (e.g., '+ tags' -> '+tags').

    This fixes the dbt1060 error: "Ignored unexpected key '+ tags'".
    When users accidentally add a space after the '+' in config keys, it creates
    an invalid key. This function:
    - Fixes valid keys by removing the space (e.g., '+ tags:' -> '+tags:')
    - Removes invalid keys entirely (keys not in the schema), including their values
    """
    return _FixSpaceAfterPlusImpl(content, config).execute()


class _FixSpaceAfterPlusImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self._refactors: list[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="fix_space_after_plus",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
        )

    def _process(self) -> str:
        # Pattern to match keys with space after plus: "+ key:" at the start of the line (after indentation)
        pattern = re.compile(r"^(\s*)\+\s+(\w+)(\s*:)", re.MULTILINE)

        matches = list(pattern.finditer(self.yml_str))

        if not matches:
            return self.yml_str

        # Collect all valid config keys from schema specs (with + prefix)
        all_valid_config_keys = set()
        for node_type, node_fields in self.schema_specs.dbtproject_specs_per_node_type.items():
            all_valid_config_keys.update(node_fields.allowed_config_fields_dbt_project_with_plus)

        # Separate matches into valid (fix) and invalid (remove) keys
        # Process in reverse order to maintain correct offsets when removing/replacing
        matches_with_action = []
        for match in matches:
            key_name = match.group(2)
            corrected_key = f"+{key_name}"
            line_num = self.yml_str[: match.start()].count("\n") + 1

            if corrected_key in all_valid_config_keys:
                matches_with_action.append(("fix", match, corrected_key, key_name, line_num))
            else:
                matches_with_action.append(("remove", match, corrected_key, key_name, line_num))

        # Sort by position in reverse to process from end to start (to maintain positions)
        matches_with_action.sort(key=lambda x: x[1].start(), reverse=True)

        refactored_yaml = self.yml_str

        for action, match, corrected_key, key_name, line_num in matches_with_action:
            if action == "fix":
                indent = match.group(1)
                colon_and_space = match.group(3)
                corrected_full = f"{indent}{corrected_key}{colon_and_space}"

                start_pos = match.start()
                end_pos = match.end()

                refactored_yaml = refactored_yaml[:start_pos] + corrected_full + refactored_yaml[end_pos:]

                self._refactored = True
                self._refactors.insert(
                    0,
                    DbtDeprecationRefactor(
                        log=f"Removed space after '+' in key '+ {key_name}' on line {line_num}, changed to '{corrected_key}'",
                        change_type=ChangeType.SPACE_AFTER_PLUS_FIXUP,
                        original_location=Location(line=line_num),
                        edited_location=Location(line=line_num),
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

                self._refactored = True
                self._refactors.insert(
                    0,
                    DbtDeprecationRefactor(
                        log=f"Removed invalid key '+ {key_name}' on line {line_num} (not a valid config key)",
                        change_type=ChangeType.INVALID_KEY_AFTER_PLUS_REMOVED,
                        original_location=Location(line=line_num),
                    ),
                )

        return refactored_yaml
