import difflib
import re
from copy import deepcopy
from typing import Dict, List, Tuple

import yaml
import yamllint.linter
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.constants import COMMON_CONFIG_MISSPELLINGS, COMMON_PROPERTY_MISSPELLINGS
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    Location,
    YMLContent,
    YMLRefactorConfig,
    YMLRuleRefactorResult,
)
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str, get_dict, get_list, load_yaml, yaml_config

NUM_SPACES_TO_REPLACE_TAB = 2


def changeset_replace_fancy_quotes(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Replace fancy quotes with appropriate handling based on context.

    Fancy quotes (U+201C ", U+201D ") are handled differently based on their position:
    - Fancy quotes used as YAML delimiters: Replaced with regular quotes
    - Fancy quotes inside string values: Preserved using placeholders (restored later)
    """
    return _ReplaceFancyQuotesImpl(content, config).execute()


class _ReplaceFancyQuotesImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="replace_fancy_quotes",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        if "\u201c" not in self.yml_str and "\u201d" not in self.yml_str:
            return self.yml_str

        lines = self.yml_str.split("\n")
        refactored_lines = []

        for line_num, line in enumerate(lines, 1):
            if "\u201c" not in line and "\u201d" not in line:
                refactored_lines.append(line)
                continue

            new_line, line_refactored, inside_string_positions = self._process_line_fancy_quotes(line)

            if line_refactored:
                self._refactored = True
                delimiter_count = (line.count("\u201c") + line.count("\u201d")) - len(inside_string_positions)
                if delimiter_count > 0:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Replaced {delimiter_count} fancy quotes with regular quotes on line {line_num}",
                            original_location=Location(line=line_num),
                            edited_location=Location(line=line_num),
                        )
                    )

            refactored_lines.append(new_line)

        return "\n".join(refactored_lines)

    @staticmethod
    def _process_line_fancy_quotes(line: str) -> Tuple[str, bool, List[int]]:
        r"""Process a single line to handle fancy quotes based on context.

        Returns:
            - The processed line
            - Whether the line was modified
            - List of positions where fancy quotes are inside strings (preserved)
        """
        result = []
        inside_string = False
        string_start_char = None  # Track what character started the current string
        inside_string_positions = []
        refactored = False
        i = 0

        while i < len(line):
            char = line[i]

            # Check if this is an escaped character
            is_escaped = i > 0 and line[i - 1] == "\\"

            # Handle regular double quote
            if char == '"' and not is_escaped:
                if not inside_string:
                    # Starting a string with regular quote
                    inside_string = True
                    string_start_char = '"'
                elif string_start_char == '"':
                    # Ending a string that was started with regular quote
                    inside_string = False
                    string_start_char = None
                elif string_start_char == "\u201c":
                    # Regular quote inside a fancy-quote-delimited string.
                    # If there's a fancy closing quote later, this is content — escape it.
                    # Otherwise it's a mismatched closing delimiter.
                    if "\u201d" in line[i + 1 :]:
                        refactored = True
                        result.append("\\")
                    else:
                        # Mismatched pair: fancy open + regular close
                        inside_string = False
                        string_start_char = None
                result.append(char)
                i += 1
                continue

            # Handle fancy left quote
            if char == "\u201c":
                if not inside_string:
                    # Fancy quote used as opening delimiter - replace with regular quote
                    refactored = True
                    inside_string = True
                    string_start_char = "\u201c"
                    result.append('"')
                elif string_start_char == '"':
                    # Fancy quote inside a regular-quote-delimited string - keep as-is (not a delimiter)
                    result.append(char)
                    inside_string_positions.append(i)
                else:
                    # Fancy quote inside a fancy-quote-delimited string - this is content, replace with regular quote
                    refactored = True
                    result.append('"')
                i += 1
                continue

            # Handle fancy right quote
            if char == "\u201d":
                if inside_string and string_start_char in ('"', "\u201c"):
                    # This could be closing a string or content inside a string
                    if (
                        string_start_char == "\u201c"
                        or string_start_char == "\u201d"
                        or (string_start_char == '"' and _ReplaceFancyQuotesImpl._would_close_string(line, i))
                    ):
                        # Fancy quote used as closing delimiter - replace with regular quote
                        refactored = True
                        result.append('"')
                        if string_start_char in ("\u201c", '"'):
                            inside_string = False
                            string_start_char = None
                    elif string_start_char == '"':
                        # Fancy quote inside a regular-quote-delimited string - keep as-is
                        result.append(char)
                        inside_string_positions.append(i)
                    else:
                        # Replace with regular quote
                        refactored = True
                        result.append('"')
                elif not inside_string:
                    # Fancy quote used as delimiter when not in string - replace
                    refactored = True
                    result.append('"')
                else:
                    # Keep as-is if inside a regular-quoted string
                    result.append(char)
                    inside_string_positions.append(i)
                i += 1
                continue

            result.append(char)
            i += 1

        return "".join(result), refactored, inside_string_positions

    @staticmethod
    def _would_close_string(line: str, pos: int) -> bool:
        """Check if a fancy right quote at position pos would close a string.

        This is a simple heuristic: if there's no regular closing quote after this position,
        then this fancy quote is likely the closing delimiter.
        """
        # Look ahead to see if there's a regular closing quote
        remaining = line[pos + 1 :]
        # If there's no regular quote after, this fancy quote is likely the closer
        return '"' not in remaining


def changeset_owner_properties_yml_str(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Generates a refactored YAML string from a single YAML file
    - moves all the owner fields that are not in owner_properties under config.meta
    """
    return _OwnerPropertiesImpl(content, config).execute()


class _OwnerPropertiesImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="restructure_owner_properties",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        for node_type in self.schema_specs.nodes_with_owner:
            if node_type in self.yml_dict:
                for node in get_list(self.yml_dict, node_type):
                    self._restructure_owner_node(node, node_type)

    def _restructure_owner_node(self, node: CommentedMap, node_type: str) -> None:
        pretty_node_type = node_type[:-1].title()

        if "owner" in node and isinstance(node["owner"], dict):
            owner = node["owner"]
            owner_copy = owner.copy()

            for field in owner_copy:
                if field not in self.schema_specs.owner_properties:
                    self._refactored = True
                    if "config" not in node:
                        node["config"] = {"meta": {}}
                    if "meta" not in node["config"]:
                        node["config"]["meta"] = {}
                    node["config"]["meta"][field] = owner[field]
                    del owner[field]
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node['name']}' - Owner field '{field}' moved under config.meta.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        )
                    )


def changeset_remove_tab_only_lines(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Remove lines that contain only tabs from YAML files."""
    return _RemoveTabOnlyLinesImpl(content, config).execute()


class _RemoveTabOnlyLinesImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_tab_only_lines",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        lines = self.yml_str.splitlines()
        new_lines = []
        for i, line in enumerate(lines):
            if "\t" in line and line.strip() == "":
                self._refactored = True
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed line containing only tabs on line {i + 1}",
                        original_location=Location(line=i + 1),
                    )
                )
                new_lines.append("")
            else:
                new_lines.append(line)

        return "\n".join(new_lines) if self._refactored else self.yml_str


def changeset_remove_indentation_version(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Standardizes the format of 'version: 2' in YAML files.

    This function looks for any variations of whitespace around 'version: 2' and
    standardizes them to the format 'version: 2'.
    """
    return _RemoveIndentationVersionImpl(content, config).execute()


class _RemoveIndentationVersionImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="removed_extra_indentation",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        pattern = r"^\s*version\s*:\s*2"
        replacement = "version: 2"

        lines = self.yml_str.splitlines()
        for i, line in enumerate(lines):
            if re.match(pattern, line):
                if line != replacement:
                    self._refactored = True
                    lines[i] = replacement
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Removed the extra indentation around 'version: 2' on line {i + 1}",
                            original_location=Location(line=i + 1),
                            edited_location=Location(line=i + 1),
                        )
                    )

        return "\n".join(lines) if self._refactored else self.yml_str


def changeset_remove_extra_tabs(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Removes extra tabs in the YAML files"""
    return _RemoveExtraTabsImpl(content, config).execute()


class _RemoveExtraTabsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_extra_tabs",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        current_yaml = self.yml_str

        while True:
            found_tab_error = False
            for p in yamllint.linter.run(current_yaml, yaml_config):
                if "found character '\\t' that cannot start any token" in p.desc:
                    found_tab_error = True
                    self._refactored = True
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Found extra tabs: line {p.line} - column {p.column}",
                            original_location=Location(line=p.line, start=p.column),
                            edited_location=Location(line=p.line, start=p.column),
                        )
                    )
                    lines = current_yaml.split("\n")
                    if p.line <= len(lines):
                        line = lines[p.line - 1]
                        if p.column <= len(line):
                            new_line = line[: p.column - 1] + " " * NUM_SPACES_TO_REPLACE_TAB + line[p.column :]
                            lines[p.line - 1] = new_line
                            current_yaml = "\n".join(lines)
                            break

            if not found_tab_error:
                return current_yaml


def changeset_refactor_yml_str(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Generates a refactored YAML string from a single YAML file
    - moves all the config fields under config
    - moves all the meta fields under config.meta and merges with existing config.meta
    - moves all the unknown fields under config.meta
    - provide some information if some fields don't exist but are similar to allowed fields
    - removes custom top-level keys
    """
    return _RefactorYMLStrImpl(content, config).execute()


class _RefactorYMLStrImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="restructure_yaml_keys",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        yml_dict_keys = list(self.yml_dict.keys())
        for key in yml_dict_keys:
            if key not in self.schema_specs.valid_top_level_yaml_fields:
                self._refactored = True
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed custom top-level key: '{key}'",
                        deprecation=DeprecationType.CUSTOM_TOP_LEVEL_KEY_DEPRECATION,
                    )
                )
                self.yml_dict.pop(key)

        for node_type in self.schema_specs.yaml_specs_per_node_type:
            if node_type in self.yml_dict:
                for i, node in enumerate(get_list(self.yml_dict, node_type)):
                    processed_node, node_refactored = self._restructure_yaml_keys_for_node(node, node_type)
                    if node_refactored:
                        self._refactored = True
                        self.yml_dict[node_type][i] = processed_node

                    if "columns" in processed_node:
                        for column_i, column in enumerate(node["columns"]):
                            processed_column, column_refactored = self._restructure_yaml_keys_for_node(
                                column, "columns"
                            )
                            if column_refactored:
                                self._refactored = True
                                self.yml_dict[node_type][i]["columns"][column_i] = processed_column

                            some_tests = {"tests", "data_tests"} & set(processed_column)
                            if some_tests:
                                test_key = next(iter(some_tests))
                                for test_i, test in enumerate(node["columns"][column_i][test_key]):
                                    processed_test, test_refactored = self._restructure_yaml_keys_for_test(test)
                                    if test_refactored:
                                        self._refactored = True
                                        self.yml_dict[node_type][i]["columns"][column_i][test_key][test_i] = (
                                            processed_test
                                        )

                    some_tests = {"tests", "data_tests"} & set(processed_node)
                    if some_tests:
                        test_key = next(iter(some_tests))
                        for test_i, test in enumerate(node[test_key]):
                            processed_test, test_refactored = self._restructure_yaml_keys_for_test(test)
                            if test_refactored:
                                self._refactored = True
                                self.yml_dict[node_type][i][test_key][test_i] = processed_test

                    if "versions" in processed_node:
                        for version_i, version in enumerate(node["versions"]):
                            some_tests = {"tests", "data_tests"} & set(version)
                            if some_tests:
                                test_key = next(iter(some_tests))
                                for test_i, test in enumerate(version[test_key]):
                                    processed_test, test_refactored = self._restructure_yaml_keys_for_test(test)
                                    if test_refactored:
                                        self._refactored = True
                                        self.yml_dict[node_type][i]["versions"][version_i][test_key][test_i] = (
                                            processed_test
                                        )

        if "sources" in self.yml_dict:
            for i, source in enumerate(self.yml_dict["sources"]):
                if "tables" in source:
                    for j, table in enumerate(source["tables"]):
                        processed_source_table, source_table_refactored = self._restructure_yaml_keys_for_node(
                            table, "tables"
                        )
                        if source_table_refactored:
                            self._refactored = True
                            self.yml_dict["sources"][i]["tables"][j] = processed_source_table

                        some_tests = {"tests", "data_tests"} & set(processed_source_table)
                        if some_tests:
                            test_key = next(iter(some_tests))
                            for test_i, test in enumerate(source["tables"][j][test_key]):
                                processed_test, test_refactored = self._restructure_yaml_keys_for_test(test)
                                if test_refactored:
                                    self._refactored = True
                                    self.yml_dict["sources"][i]["tables"][j][test_key][test_i] = processed_test

                        if "columns" in processed_source_table:
                            for table_column_i, table_column in enumerate(table["columns"]):
                                processed_table_column, table_column_refactored = self._restructure_yaml_keys_for_node(
                                    table_column, "columns"
                                )
                                if table_column_refactored:
                                    self._refactored = True
                                    self.yml_dict["sources"][i]["tables"][j]["columns"][table_column_i] = (
                                        processed_table_column
                                    )

                                some_tests = {"tests", "data_tests"} & set(processed_table_column)
                                if some_tests:
                                    test_key = next(iter(some_tests))
                                    for test_i, test in enumerate(table_column[test_key]):
                                        processed_test, test_refactored = self._restructure_yaml_keys_for_test(test)
                                        if test_refactored:
                                            self._refactored = True
                                            self.yml_dict["sources"][i]["tables"][j]["columns"][table_column_i][
                                                test_key
                                            ][test_i] = processed_test

    def _restructure_yaml_keys_for_test(self, test: CommentedMap) -> Tuple[CommentedMap, bool]:
        if isinstance(test, str):
            return test, False

        test_name = next(iter(test.keys()))
        if isinstance(test[test_name], dict):
            test_definition = test[test_name]
        else:
            test_name = test["test_name"]
            test_definition = test

        before_count = len(self._refactors)
        self._refactor_test_common_misspellings(test_definition, test_name)
        self._refactor_test_config_fields(test_definition, test_name)
        self._refactor_test_args(test_definition, test_name)

        return test, len(self._refactors) > before_count

    def _refactor_test_config_fields(self, test_definition: CommentedMap, test_name: str) -> None:
        test_configs = self.schema_specs.yaml_specs_per_node_type["tests"].allowed_config_fields
        test_properties = self.schema_specs.yaml_specs_per_node_type["tests"].allowed_properties

        copy_test_definition = deepcopy(test_definition)
        for field in copy_test_definition:
            if field == "partition_by" and test_name == "dbt_utils.mutually_exclusive_ranges":
                continue

            if field in test_configs and field not in test_properties:
                node_config = get_dict(test_definition, "config")

                if field not in node_config:
                    node_config.update({field: test_definition[field]})
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Test '{test_name}' - Field '{field}' moved under config.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        )
                    )
                    test_definition["config"] = node_config
                else:
                    node_config[field] = test_definition[field]
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"Test '{test_name}' - Field '{field}' is already under config, it has been overwritten and removed from the top level.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        )
                    )
                    test_definition["config"] = node_config
                del test_definition[field]

    def _refactor_test_common_misspellings(self, test_definition: CommentedMap, test_name: str) -> None:
        for field in test_definition:
            if field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Test '{test_name}' - Field '{field}' is a common misspelling of '{COMMON_PROPERTY_MISSPELLINGS[field.lower()]}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
                test_definition[COMMON_PROPERTY_MISSPELLINGS[field.lower()]] = test_definition[field]
                del test_definition[field]

    def _refactor_test_args(self, test_definition: CommentedMap, test_name: str) -> None:
        """Move non-config args under 'arguments' key
        This refactor is only necessary for custom tests, or tests making use of the alternative test definition syntax ('test_name')
        """
        copy_test_definition = deepcopy(test_definition)
        if "arguments" in test_definition and not isinstance(test_definition["arguments"], dict):
            return

        for field in copy_test_definition:
            if field in ("config", "arguments", "test_name", "name", "description", "column_name"):
                continue
            self._refactors.append(
                DbtDeprecationRefactor(
                    log=f"Test '{test_name}' - Custom test argument '{field}' moved under 'arguments'.",
                    deprecation=DeprecationType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION,
                )
            )
            test_definition["arguments"] = get_dict(test_definition, "arguments")
            test_definition["arguments"].update({field: test_definition[field]})
            del test_definition[field]

    def _restructure_yaml_keys_for_node(self, node: CommentedMap, node_type: str) -> Tuple[CommentedMap, bool]:
        refactored = False
        existing_meta = get_dict(node, "meta").copy()
        existing_config = get_dict(node, "config").copy()
        pretty_node_type = node_type[:-1].title()

        for field in existing_config:
            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields or field in (
                "target_schema",
                "target_database",
            ):
                continue

            refactored = True
            if field in COMMON_CONFIG_MISSPELLINGS:
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is a common misspelling of '{COMMON_CONFIG_MISSPELLINGS[field]}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                    )
                )
                node["config"][COMMON_CONFIG_MISSPELLINGS[field]] = node["config"][field]
                del node["config"][field]
            else:
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is not an allowed config - Moved under config.meta.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                    )
                )
                node_config_meta = get_dict(get_dict(node, "config"), "meta")
                node_config_meta.update({field: node["config"][field]})
                node["config"] = get_dict(node, "config")
                node["config"].update({"meta": node_config_meta})
                del node["config"][field]

        copy_node = node.copy()

        for field in copy_node:
            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_properties:
                continue
            elif field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
                refactored = True
                correct_field = COMMON_PROPERTY_MISSPELLINGS[field.lower()]
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is a common misspelling of '{correct_field}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
                node[correct_field] = node[field]
                del node[field]
                continue

            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields_without_meta:
                refactored = True
                node_config = get_dict(node, "config")

                if field not in node_config:
                    node_config.update({field: node[field]})
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' moved under config.",
                            deprecation=DeprecationType.PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                        )
                    )
                    node["config"] = node_config
                else:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is already under config, it has been removed from the top level.",
                            deprecation="PropertyMovedToConfigDeprecation",
                        )
                    )
                del node[field]

            if field not in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields:
                refactored = True
                closest_match = difflib.get_close_matches(
                    str(field),
                    self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields.union(
                        set(self.schema_specs.yaml_specs_per_node_type[node_type].allowed_properties)
                    ),
                    1,
                )
                if closest_match:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not allowed, but '{closest_match[0]}' is. Moved as-is under config.meta but you might want to rename it and move it under config.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        )
                    )
                else:
                    self._refactors.append(
                        DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not an allowed config - Moved under config.meta.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        )
                    )
                node_meta = get_dict(get_dict(node, "config"), "meta")
                node_meta.update({field: node[field]})
                node["config"] = get_dict(node, "config")
                node["config"].update({"meta": node_meta})
                del node[field]

        if existing_meta:
            refactored = True
            self._refactors.append(
                DbtDeprecationRefactor(
                    log=f"{pretty_node_type} '{node.get('name', '')}' - Moved all the meta fields under config.meta and merged with existing config.meta.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                )
            )

            if "config" not in node:
                node["config"] = {"meta": {}}
            if "meta" not in node["config"]:
                node["config"]["meta"] = {}
            for key, value in existing_meta.items():
                node["config"]["meta"].update({key: value})
            del node["meta"]

        return node, refactored


def changeset_replace_non_alpha_underscores_in_name_values(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _ReplaceNonAlphaUnderscoresImpl(content, config).execute()


class _ReplaceNonAlphaUnderscoresImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: List[DbtDeprecationRefactor] = []

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        refactored = len(self._refactors) > 0
        return YMLRuleRefactorResult(
            rule_name="remove_spaces_in_resource_names",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if refactored else self.yml_str,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> None:
        for node_type in self.schema_specs.yaml_specs_per_node_type:
            if node_type in self.yml_dict:
                for i, node in enumerate(get_list(self.yml_dict, node_type)):
                    processed_node, node_deprecation_refactors = self._replace_node_name_non_alpha_with_underscores(
                        node, node_type
                    )
                    if node_deprecation_refactors:
                        self.yml_dict[node_type][i] = processed_node
                        self._refactors.extend(node_deprecation_refactors)

    @staticmethod
    def _replace_spaces_outside_jinja(text: str) -> str:
        """Replace spaces with underscores, but preserve spaces inside Jinja templates.

        This function avoids corrupting Jinja templates like {{ env_var('X') | lower }}
        by only replacing spaces that are outside of {{ }} blocks.

        Args:
            text: The text to process

        Returns:
            Text with spaces replaced by underscores, except inside Jinja templates
        """
        result = []
        i = 0
        in_jinja = False

        while i < len(text):
            # Check for Jinja opening {{
            if i < len(text) - 1 and text[i : i + 2] == "{{":
                in_jinja = True
                result.append("{{")
                i += 2
                continue

            # Check for Jinja closing }}
            if i < len(text) - 1 and text[i : i + 2] == "}}":
                in_jinja = False
                result.append("}}")
                i += 2
                continue

            # Replace spaces with underscores only outside Jinja blocks
            if text[i] == " " and not in_jinja:
                result.append("_")
            else:
                result.append(text[i])

            i += 1

        return "".join(result)

    @staticmethod
    def _remove_non_alpha_outside_jinja(text: str) -> str:
        """Remove non-alphanumeric characters (except underscores), but preserve Jinja templates.

        This function avoids corrupting Jinja templates like {{ env_var('X') | lower }}
        by preserving everything inside {{ }} blocks.

        Args:
            text: The text to process

        Returns:
            Text with non-alphanumeric characters removed, except inside Jinja templates
        """
        result = []
        i = 0
        jinja_depth = 0

        while i < len(text):
            # Check for Jinja opening {{
            if i < len(text) - 1 and text[i : i + 2] == "{{":
                jinja_depth += 1
                result.append("{{")
                i += 2
                continue

            # Check for Jinja closing }}
            if i < len(text) - 1 and text[i : i + 2] == "}}":
                result.append("}}")
                jinja_depth -= 1
                i += 2
                continue

            # Keep character if it's alphanumeric/underscore, or if we're inside Jinja
            char = text[i]
            if jinja_depth > 0 or char.isalnum() or char == "_":
                result.append(char)
            # Otherwise skip the character (it's removed)

            i += 1

        return "".join(result)

    @staticmethod
    def _replace_node_name_non_alpha_with_underscores(node: CommentedMap, node_type: str):
        node_deprecation_refactors: List[DbtDeprecationRefactor] = []
        node_copy = node.copy()
        pretty_node_type = node_type[:-1].title()

        deprecation = None
        name = node.get("name", None)
        new_name = None
        if name:
            if node_type == "exposures":
                new_name = _ReplaceNonAlphaUnderscoresImpl._replace_spaces_outside_jinja(name)
                new_name = _ReplaceNonAlphaUnderscoresImpl._remove_non_alpha_outside_jinja(new_name)
                deprecation = "ExposureNameDeprecation"
            else:
                new_name = _ReplaceNonAlphaUnderscoresImpl._replace_spaces_outside_jinja(name)
                deprecation = "ResourceNamesWithSpacesDeprecation"

            if new_name and new_name != name:
                node_copy["name"] = new_name
                node_deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node['name']}' - Updated 'name' from '{name}' to '{new_name}'.",
                        deprecation=deprecation,
                    )
                )

        return node_copy, node_deprecation_refactors


def changeset_remove_duplicate_models(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Removes duplicate model definitions in YAML files, keeping the last occurrence.

    When the same model name appears multiple times in the models list, this function
    removes all but the last occurrence, aligning with dbt's behavior of keeping the
    last definition when duplicates exist.
    """
    return _RemoveDuplicateModelsImpl(content, config).execute()


class _RemoveDuplicateModelsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self.yml_dict = load_yaml(self.yml_str)
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_duplicate_models",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        if "models" not in self.yml_dict or not isinstance(self.yml_dict["models"], list):
            return self.yml_str

        seen_model_names: Dict[str, List[int]] = {}
        indices_to_remove: List[int] = []

        for i, model in enumerate(self.yml_dict["models"]):
            if not isinstance(model, dict):
                continue
            model_name = model.get("name")
            if model_name is None:
                continue
            if model_name not in seen_model_names:
                seen_model_names[model_name] = []
            seen_model_names[model_name].append(i)

        for model_name, indices in seen_model_names.items():
            if len(indices) > 1:
                self._refactored = True
                indices_to_remove.extend(indices[:-1])
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Model '{model_name}' - Found duplicate definition, removed first occurrence (keeping the second one).",
                        deprecation=DeprecationType.DUPLICATE_YAML_KEYS_DEPRECATION,
                    )
                )

        if self._refactored:
            indices_to_remove.sort(reverse=True)
            for index in indices_to_remove:
                self.yml_dict["models"].pop(index)
            return dict_to_yaml_str(self.yml_dict)

        return self.yml_str


def changeset_remove_duplicate_keys(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    """Removes duplicate keys in the YAML files, keeping the first occurrence only.

    The drawback of keeping the first occurrence is that we need to use PyYAML and then lose all the comments that were in the file
    """
    return _RemoveDuplicateKeysImpl(content, config).execute()


class _RemoveDuplicateKeysImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.config = config
        self._refactors: List[DbtDeprecationRefactor] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        refactored_yaml = self._process()
        return YMLRuleRefactorResult(
            rule_name="remove_duplicate_keys",
            refactored=self._refactored,
            refactored_yaml=refactored_yaml,
            original_yaml=self.yml_str,
            deprecation_refactors=self._refactors,
        )

    def _process(self) -> str:
        for p in yamllint.linter.run(self.yml_str, yaml_config):
            if p.rule == "key-duplicates":
                self._refactored = True
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Found duplicate keys: line {p.line} - {p.desc}",
                        deprecation="DuplicateYAMLKeysDeprecation",
                    )
                )

        if self._refactored:
            return DbtYAML().dump_to_string(yaml.safe_load(self.yml_str))

        return self.yml_str
