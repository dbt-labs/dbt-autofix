import re
from pathlib import Path
from typing import Any, Optional

import yamllint.config

from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    DbtProjectYMLRefactorConfig,
    YMLContent,
    YMLRuleRefactorResult,
)
from dbt_autofix.refactors.yml import DbtYAML, get_dict, load_yaml
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
        self.yml_str = content.current_str
        self.config = config
        self.exclude_dbt_project_keys = config.exclude_dbt_project_keys
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: list[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_deprecated_config",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
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
            "log-path": "ConfigLogPathDeprecation",
            "target-path": "ConfigTargetPathDeprecation",
            "data-paths": "ConfigDataPathDeprecation",
            "source-paths": "ConfigSourcePathDeprecation",
        }

        for deprecated_field, _ in dict_deprecated_fields_with_defaults.items():
            if deprecated_field in self.yml_dict:
                if not self.exclude_dbt_project_keys:
                    # by default we remove it
                    self._refactored = True
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Removed the deprecated field '{deprecated_field}'",
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        )
                    )
                    del self.yml_dict[deprecated_field]
                # with the special field, we only remove it if it's different from the default
                elif self.yml_dict[deprecated_field] != dict_deprecated_fields_with_defaults[deprecated_field]:
                    self._refactored = True
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Removed the deprecated field '{deprecated_field}' that wasn't set to the default value",
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        )
                    )
                    del self.yml_dict[deprecated_field]

        # TODO: add tests for this
        for deprecated_field, new_field in dict_renamed_fields.items():
            if deprecated_field in self.yml_dict:
                self._refactored = True
                if new_field not in self.yml_dict:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Renamed the deprecated field '{deprecated_field}' to '{new_field}'",
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        )
                    )
                    self.yml_dict[new_field] = self.yml_dict[deprecated_field]
                else:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Added the config of the deprecated field '{deprecated_field}' to '{new_field}'",
                            deprecation=dict_fields_to_deprecation_class[deprecated_field],
                        )
                    )
                    self.yml_dict[new_field] = self.yml_dict[new_field] + self.yml_dict[deprecated_field]
                del self.yml_dict[deprecated_field]


def _path_exists_as_file(path: Path) -> bool:
    return path.with_suffix(".py").exists() or path.with_suffix(".sql").exists() or path.with_suffix(".csv").exists()


def changeset_dbt_project_prefix_plus_for_config(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    """Update keys for the config in dbt_project.yml under to prefix it with a `+`"""
    return _PrefixPlusForConfigImpl(content, config).execute()


class _PrefixPlusForConfigImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.root_path = config.root_path
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: list[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="prefix_plus_for_config",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        for node_type, node_fields in self.schema_specs.dbtproject_specs_per_node_type.items():
            for k, v in get_dict(self.yml_dict, node_type).copy().items():
                # check if this is the project name
                if k == self.yml_dict["name"]:
                    # Only recurse if v is a dict (should be project configs)
                    if isinstance(v, dict):
                        self.yml_dict[node_type][k] = self._rec_check_yaml_path(
                            v, self.root_path / node_type, node_fields, node_type
                        )
                    # else: non-dict value, keep as-is (unusual but possible)

                # top level config (with or without + prefix)
                elif k in node_fields.allowed_config_fields_dbt_project or (
                    k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
                ):
                    # Config key is valid - if it doesn't have +, add it
                    if not k.startswith("+"):
                        new_k = f"+{k}"
                        self._refactors.append(
                            DbtDeprecationRefactor(
                                log=f"Added '+' in front of top level config '{k}'",
                                deprecation="MissingPlusPrefixDeprecation",
                            )
                        )
                        self._refactored = True
                        self.yml_dict[node_type][new_k] = v
                        del self.yml_dict[node_type][k]
                    # else: already has +, keep as-is, value is the config value (don't recurse)

                # otherwise, treat it as a package or logical grouping
                # TODO: if this is not valid, we could delete it as well
                else:
                    packages_path = self.root_path / Path(self.yml_dict.get("packages-paths", "dbt_packages"))
                    # Only recurse if v is a dict (should be package configs or logical grouping)
                    if isinstance(v, dict):
                        self.yml_dict[node_type][k] = self._rec_check_yaml_path(
                            v,
                            packages_path / k / node_type,
                            node_fields,
                            node_type,
                        )
                    # else: non-dict value, keep as-is (unusual but possible)

    def _rec_check_yaml_path(
        self,
        yml_dict: Any,
        path: Path,
        node_fields: DbtProjectSpecs,
        node_type: Optional[str] = None,
    ) -> Any:
        # TODO: what about individual models in the config there?
        # indivdual models would show up here but without the `.sql` (or `.py`)

        # Type guard: if yml_dict is not a dict, return it as-is
        # This handles cases where config values are lists, ints, strings, bools, etc.
        # For example: partition_by={'field': 'x', 'range': {...}}, cluster_by=['col1', 'col2']
        if not isinstance(yml_dict, dict):
            return yml_dict

        yml_dict_copy = yml_dict.copy() if yml_dict else {}
        for k, v in yml_dict_copy.items():
            if not (path / k).exists() and not _path_exists_as_file(path / k):
                # Case 1: Key doesn't have "+" prefix
                if not k.startswith("+"):
                    if k in node_fields.allowed_config_fields_dbt_project:
                        # Built-in config missing "+": rename in-place
                        new_k = f"+{k}"
                        yml_dict[new_k] = v
                        del yml_dict[k]
                        self._refactors.append(
                            DbtDeprecationRefactor(
                                log=f"Added '+' in front of the nested config '{k}'",
                                deprecation="MissingPlusPrefixDeprecation",
                            )
                        )
                        self._refactored = True
                    elif isinstance(v, dict):
                        # Logical grouping (subdirectory-like structure): recurse
                        yml_dict[k] = self._rec_check_yaml_path(v, path / k, node_fields, node_type)
                    else:
                        # Custom leaf config: move to +meta
                        meta = get_dict(yml_dict, "+meta")
                        meta.update({k: v})
                        yml_dict["+meta"] = meta
                        del yml_dict[k]
                        self._refactors.append(
                            DbtDeprecationRefactor(
                                log=f"Moved custom config '{k}' to '+meta'",
                                deprecation="MissingPlusPrefixDeprecation",
                            )
                        )
                        self._refactored = True

                # Case 2: Key already has "+" prefix - validate it
                else:
                    key_without_plus = k[1:]

                    if key_without_plus in node_fields.allowed_config_fields_dbt_project:
                        # Valid config: check for invalid subkeys in dict-typed configs
                        if isinstance(v, dict) and self.schema_specs is not None:
                            dict_config_analysis = self.schema_specs.get_dict_config_analysis()
                            if key_without_plus in dict_config_analysis["specific_properties"]:
                                allowed_props = dict_config_analysis["specific_properties"][key_without_plus]
                                for subkey, subvalue in v.copy().items():
                                    if subkey.startswith("+"):
                                        # +prefixed subkey in a dict config - move to +meta
                                        meta = get_dict(yml_dict, "+meta")
                                        meta[subkey] = subvalue
                                        yml_dict["+meta"] = meta
                                        del v[subkey]
                                        self._refactors.append(
                                            DbtDeprecationRefactor(
                                                log=f"Moved '{subkey}' from '{k}' to '+meta' (subkeys shouldn't be +prefixed)",
                                                deprecation="MissingPlusPrefixDeprecation",
                                            )
                                        )
                                        self._refactored = True
                                    elif subkey not in allowed_props:
                                        # Subkey not in allowed properties - move to +meta
                                        meta = get_dict(yml_dict, "+meta")
                                        meta[subkey] = subvalue
                                        yml_dict["+meta"] = meta
                                        del v[subkey]
                                        self._refactors.append(
                                            DbtDeprecationRefactor(
                                                log=f"Moved '{subkey}' from '{k}' to '+meta' (not a valid property for {key_without_plus})",
                                                deprecation="MissingPlusPrefixDeprecation",
                                            )
                                        )
                                        self._refactored = True

                    else:
                        # Unrecognized +prefixed config: strip + and move to +meta
                        meta = get_dict(yml_dict, "+meta")
                        meta.update({key_without_plus: v})
                        yml_dict["+meta"] = meta
                        del yml_dict[k]
                        self._refactors.append(
                            DbtDeprecationRefactor(
                                log=f"Moved unrecognized config '{k}' to '+meta'",
                                deprecation="MissingPlusPrefixDeprecation",
                            )
                        )
                        self._refactored = True

            # Only recurse into dict values if the path exists (real directory/logical grouping)
            # Do NOT recurse into values of valid config keys (like +persist_docs, +labels)
            elif isinstance(yml_dict[k], dict):
                is_valid_config = k.startswith("+") and k[1:] in node_fields.allowed_config_fields_dbt_project
                if not is_valid_config:
                    yml_dict[k] = self._rec_check_yaml_path(yml_dict[k], path / k, node_fields, node_type)
        return yml_dict


def changeset_dbt_project_flip_behavior_flags(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _FlipBehaviorFlagsImpl(content, config).execute()


class _FlipBehaviorFlagsImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: list[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="flip_behavior_flags",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        behavior_change_flag_to_explainations = {
            "source_freshness_run_project_hooks": "run project hooks (on-run-start/on-run-end) as part of source freshness commands"
        }

        for key in self.yml_dict:
            if key == "flags":
                for behavior_change_flag in behavior_change_flag_to_explainations:
                    if self.yml_dict["flags"].get(behavior_change_flag) is False:
                        self.yml_dict["flags"][behavior_change_flag] = True
                        self._refactored = True
                        self._refactors.append(
                            DbtDeprecationRefactor(
                                log=f"Set flag '{behavior_change_flag}' to 'True' - This will {behavior_change_flag_to_explainations[behavior_change_flag]}.",
                                deprecation="SourceFreshnessProjectHooksNotRun",
                            )
                        )


def changeset_dbt_project_flip_test_arguments_behavior_flag(
    content: YMLContent, config: DbtProjectYMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _FlipTestArgumentsBehaviorFlagImpl(content, config).execute()


class _FlipTestArgumentsBehaviorFlagImpl:
    def __init__(self, content: YMLContent, config: DbtProjectYMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: list[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="changeset_dbt_project_flip_test_arguments_behavior_flag",
            refactored=self._refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        existing_flags = get_dict(self.yml_dict, "flags")
        if (
            existing_flags.get("require_generic_test_arguments_property") is False
            or "require_generic_test_arguments_property" not in existing_flags
        ):
            self.yml_dict["flags"] = existing_flags
            self.yml_dict["flags"]["require_generic_test_arguments_property"] = True
            self._refactored = True
            self._refactors.append(
                DbtDeprecationRefactor(
                    log="Set flag 'require_generic_test_arguments_property' to 'True' - This will parse the values defined within the `arguments` property of test definition as the test keyword arguments.",
                    deprecation="MissingGenericTestArgumentsPropertyDeprecation",
                )
            )


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
            deprecation_refactors=self._refactors,
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

                self._refactored = True
                self._refactors.insert(
                    0,
                    DbtDeprecationRefactor(
                        log=f"Removed invalid key '+ {key_name}' on line {line_num} (not a valid config key)"
                    ),
                )

        return refactored_yaml
