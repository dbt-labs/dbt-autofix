import difflib
import re
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

import yamllint.linter
from ruamel.yaml.comments import CommentedMap

from dbt_autofix.deprecations import ChangeType, DeprecationType
from dbt_autofix.refactors.constants import COMMON_CONFIG_MISSPELLINGS, COMMON_PROPERTY_MISSPELLINGS
from dbt_autofix.refactors.node import assign_node, extract_node, pop_node
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    Location,
    RefactorEntry,
    YMLContent,
    YMLDeprecationRefactor,
    YMLRefactorConfig,
    YMLRuleRefactorResult,
    location_of_key,
)
from dbt_autofix.refactors.yml import (
    DbtYAML,
    copy_ca,
    dict_to_yaml_str,
    get_dict,
    get_list,
    load_yaml,
    yaml_config,
)

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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
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
                            change_type=ChangeType.FANCY_QUOTES_FIXUP,
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
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: List[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="restructure_owner_properties",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        for node_type in self.schema_specs.nodes_with_owner:
            if node_type in self.yml_dict:
                original_nodes = get_list(self.content.original_parsed, node_type)
                for i, node in enumerate(get_list(self.yml_dict, node_type)):
                    original_node = original_nodes[i] if i < len(original_nodes) else None
                    self._restructure_owner_node(node, original_node, node_type, i)

    def _restructure_owner_node(
        self, node: CommentedMap, original_node: Optional[CommentedMap], node_type: str, i: int
    ) -> None:
        pretty_node_type = node_type[:-1].title()

        if "owner" in node and isinstance(node["owner"], dict):
            owner = node["owner"]
            owner_copy = owner.copy()
            original_owner = original_node["owner"] if original_node is not None and "owner" in original_node else owner

            for field in owner_copy:
                if field not in self.schema_specs.owner_properties:
                    self._refactored = True
                    n = pop_node(owner, field, original_parent=original_owner)
                    if "config" not in node:
                        node["config"] = CommentedMap()
                    if "meta" not in node["config"]:
                        node["config"]["meta"] = CommentedMap()
                    assign_node(node["config"]["meta"], field, n)
                    self._refactor_entries.append(
                        YMLDeprecationRefactor(
                            refactor=DbtDeprecationRefactor(
                                log=f"{pretty_node_type} '{node['name']}' - Owner field '{field}' moved under config.meta.",
                                deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                                change_type=ChangeType.OWNER_FIELD_MOVED_TO_META_DEPRECATION,
                                original_location=n.original_location,
                            ),
                            edited_key_path=[node_type, i, "config", "meta", field],
                        ).to_entry()
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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
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
                        change_type=ChangeType.TAB_ONLY_LINE_FIXUP,
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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
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
                            change_type=ChangeType.EXTRA_INDENTATION_FIXUP,
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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
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
                            change_type=ChangeType.EXTRA_TABS_FIXUP,
                            original_location=Location(line=p.line, start=p.column - 1),
                            edited_location=Location(line=p.line, start=p.column - 1),
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
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: List[RefactorEntry] = []
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        return YMLRuleRefactorResult(
            rule_name="restructure_yaml_keys",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(self.yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        yml_dict_keys = list(self.yml_dict.keys())
        for key in yml_dict_keys:
            if key not in self.schema_specs.valid_top_level_yaml_fields:
                self._refactored = True
                original_location = location_of_key(self.content.original_parsed, key)
                self._refactor_entries.append(
                    RefactorEntry(
                        refactor=DbtDeprecationRefactor(
                            log=f"Removed custom top-level key: '{key}'",
                            change_type=ChangeType.CUSTOM_TOP_LEVEL_KEY_REMOVED,
                            deprecation=DeprecationType.CUSTOM_TOP_LEVEL_KEY_DEPRECATION,
                            original_location=original_location,
                        )
                    )
                )
                self.yml_dict.pop(key)

        for node_type in self.schema_specs.yaml_specs_per_node_type:
            if node_type in self.yml_dict:
                original_nodes = get_list(self.content.original_parsed, node_type)
                for i, node in enumerate(get_list(self.yml_dict, node_type)):
                    original_node = original_nodes[i] if i < len(original_nodes) else None
                    self._restructure_yaml_keys_for_node(node, original_node, node_type, path_prefix=[node_type, i])

                    if "columns" in node:
                        original_columns = get_list(original_node, "columns") if original_node is not None else []
                        for column_i, column in enumerate(node["columns"]):
                            original_column = original_columns[column_i] if column_i < len(original_columns) else None
                            self._restructure_yaml_keys_for_node(
                                column, original_column, "columns", path_prefix=[node_type, i, "columns", column_i]
                            )

                            # there might be some tests, but they can be called tests or data_tests
                            some_tests = {"tests", "data_tests"} & set(column)
                            if some_tests:
                                test_key = next(iter(some_tests))
                                original_col_tests = (
                                    get_list(original_column, test_key) if original_column is not None else []
                                )
                                for test_i, test in enumerate(column[test_key]):
                                    original_test = (
                                        original_col_tests[test_i] if test_i < len(original_col_tests) else None
                                    )
                                    self._restructure_yaml_keys_for_test(
                                        test,
                                        original_test,
                                        path_prefix=[node_type, i, "columns", column_i, test_key, test_i],
                                    )

                    # if there are tests, we need to restructure them
                    some_tests = {"tests", "data_tests"} & set(node)
                    if some_tests:
                        test_key = next(iter(some_tests))
                        original_node_tests = get_list(original_node, test_key) if original_node is not None else []
                        for test_i, test in enumerate(node[test_key]):
                            original_test = original_node_tests[test_i] if test_i < len(original_node_tests) else None
                            self._restructure_yaml_keys_for_test(
                                test, original_test, path_prefix=[node_type, i, test_key, test_i]
                            )

                    if "versions" in node:
                        original_versions = get_list(original_node, "versions") if original_node is not None else []
                        for version_i, version in enumerate(node["versions"]):
                            original_version = (
                                original_versions[version_i] if version_i < len(original_versions) else None
                            )
                            some_tests = {"tests", "data_tests"} & set(version)
                            if some_tests:
                                test_key = next(iter(some_tests))
                                original_version_tests = (
                                    get_list(original_version, test_key) if original_version is not None else []
                                )
                                for test_i, test in enumerate(version[test_key]):
                                    original_test = (
                                        original_version_tests[test_i] if test_i < len(original_version_tests) else None
                                    )
                                    self._restructure_yaml_keys_for_test(
                                        test,
                                        original_test,
                                        path_prefix=[node_type, i, "versions", version_i, test_key, test_i],
                                    )

        if "sources" in self.yml_dict:
            original_sources = get_list(self.content.original_parsed, "sources")
            for i, source in enumerate(self.yml_dict["sources"]):
                original_source = original_sources[i] if i < len(original_sources) else None
                if "tables" in source:
                    original_tables = get_list(original_source, "tables") if original_source is not None else []
                    for j, table in enumerate(source["tables"]):
                        original_table = original_tables[j] if j < len(original_tables) else None
                        self._restructure_yaml_keys_for_node(
                            table, original_table, "tables", path_prefix=["sources", i, "tables", j]
                        )

                        some_tests = {"tests", "data_tests"} & set(table)
                        if some_tests:
                            test_key = next(iter(some_tests))
                            original_table_tests = (
                                get_list(original_table, test_key) if original_table is not None else []
                            )
                            for test_i, test in enumerate(table[test_key]):
                                original_test = (
                                    original_table_tests[test_i] if test_i < len(original_table_tests) else None
                                )
                                self._restructure_yaml_keys_for_test(
                                    test,
                                    original_test,
                                    path_prefix=["sources", i, "tables", j, test_key, test_i],
                                )

                        if "columns" in table:
                            original_table_columns = (
                                get_list(original_table, "columns") if original_table is not None else []
                            )
                            for table_column_i, table_column in enumerate(table["columns"]):
                                original_table_column = (
                                    original_table_columns[table_column_i]
                                    if table_column_i < len(original_table_columns)
                                    else None
                                )
                                self._restructure_yaml_keys_for_node(
                                    table_column,
                                    original_table_column,
                                    "columns",
                                    path_prefix=["sources", i, "tables", j, "columns", table_column_i],
                                )

                                some_tests = {"tests", "data_tests"} & set(table_column)
                                if some_tests:
                                    test_key = next(iter(some_tests))
                                    original_tc_tests = (
                                        get_list(original_table_column, test_key)
                                        if original_table_column is not None
                                        else []
                                    )
                                    for test_i, test in enumerate(table_column[test_key]):
                                        original_test = (
                                            original_tc_tests[test_i] if test_i < len(original_tc_tests) else None
                                        )
                                        self._restructure_yaml_keys_for_test(
                                            test,
                                            original_test,
                                            path_prefix=[
                                                "sources",
                                                i,
                                                "tables",
                                                j,
                                                "columns",
                                                table_column_i,
                                                test_key,
                                                test_i,
                                            ],
                                        )

    def _restructure_yaml_keys_for_test(
        self,
        test: CommentedMap,
        original_test: Optional[CommentedMap],
        path_prefix: list,
    ) -> None:
        # if the test is a string, we leave it as is
        if isinstance(test, str):
            return

        # extract the test name and definition
        test_name = next(iter(test.keys()))
        if isinstance(test[test_name], dict):
            # standard test definition syntax
            test_definition = test[test_name]
            is_standard_syntax = True
        else:
            # alt syntax
            test_name = test["test_name"]
            test_definition = test
            is_standard_syntax = False

        # compute original definition for location lookups
        original_definition = None
        if original_test is not None and not isinstance(original_test, str):
            orig_test_name = next(iter(original_test.keys()))
            if isinstance(original_test[orig_test_name], dict):
                original_definition = original_test[orig_test_name]
            else:
                original_definition = original_test

        sub_path = [*path_prefix, test_name] if is_standard_syntax else path_prefix
        self._refactor_test_common_misspellings(test_definition, original_definition, test_name, sub_path)
        self._refactor_test_config_fields(test_definition, original_definition, test_name, sub_path)
        self._refactor_test_args(test_definition, original_definition, test_name, sub_path)

    def _refactor_test_config_fields(
        self,
        test_definition: CommentedMap,
        original_definition: Optional[CommentedMap],
        test_name: str,
        path_prefix: list,
    ) -> None:
        test_configs = self.schema_specs.yaml_specs_per_node_type["tests"].allowed_config_fields
        test_properties = self.schema_specs.yaml_specs_per_node_type["tests"].allowed_properties
        _def_for_loc = original_definition if original_definition is not None else test_definition

        copy_test_definition = deepcopy(test_definition)
        for field in copy_test_definition:
            # dbt_utils.mutually_exclusive_ranges accepts partition_by as an argument
            # https://github.com/dbt-labs/dbt-utils/blob/0feb9571187119dc48203ad91d8b064a660d6d3a/macros/generic_tests/mutually_exclusive_ranges.sql#L5
            if field == "partition_by" and test_name == "dbt_utils.mutually_exclusive_ranges":
                continue

            # field is a config and not a property
            if field in test_configs and field not in test_properties:
                n = pop_node(test_definition, field, original_parent=_def_for_loc)
                node_config = test_definition.get("config", {})

                # if the field is not under config, move it under config
                if field not in node_config:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"Test '{test_name}' - Field '{field}' moved under config.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                            change_type=ChangeType.PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", field],
                    )
                # if the field is already under config, overwrite it and remove from top level
                else:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"Test '{test_name}' - Field '{field}' is already under config, it has been overwritten and removed from the top level.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                            change_type=ChangeType.PROPERTY_OVERWRITTEN_IN_CONFIG_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", field],
                    )
                assign_node(test_definition.setdefault("config", CommentedMap()), field, n)
                self._refactored = True
                self._refactor_entries.append(yr.to_entry())

    def _refactor_test_common_misspellings(
        self,
        test_definition: CommentedMap,
        original_definition: Optional[CommentedMap],
        test_name: str,
        path_prefix: list,
    ) -> None:
        _def_for_loc = original_definition if original_definition is not None else test_definition

        for field in test_definition:
            if field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
                correct_field = COMMON_PROPERTY_MISSPELLINGS[field.lower()]
                position = list(test_definition.keys()).index(field)
                n = pop_node(test_definition, field, original_parent=_def_for_loc)
                yr = YMLDeprecationRefactor(
                    refactor=DbtDeprecationRefactor(
                        log=f"Test '{test_name}' - Field '{field}' is a common misspelling of '{correct_field}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        change_type=ChangeType.PROPERTY_MISSPELLING_DEPRECATION,
                        original_location=n.original_location,
                    ),
                    edited_key_path=[*path_prefix, correct_field],
                )
                assign_node(test_definition, correct_field, n, position=position)
                self._refactored = True
                self._refactor_entries.append(yr.to_entry())

    def _refactor_test_args(
        self,
        test_definition: CommentedMap,
        original_definition: Optional[CommentedMap],
        test_name: str,
        path_prefix: list,
    ) -> None:
        r"""Move non-config args under 'arguments' key.
        This refactor is only necessary for custom tests, or tests making use of the alternative test definition syntax ('test_name').
        """
        _def_for_loc = original_definition if original_definition is not None else test_definition

        copy_test_definition = deepcopy(test_definition)
        # Avoid refactoring if the test already has an arguments key that is not a dict
        if "arguments" in test_definition and not isinstance(test_definition["arguments"], dict):
            return

        for field in copy_test_definition:
            # TODO: pull from CustomTestMultiKey on schema_specs once available in jsonschemas
            if field in ("config", "arguments", "test_name", "name", "description", "column_name"):
                continue
            n = pop_node(test_definition, field, original_parent=_def_for_loc)
            yr = YMLDeprecationRefactor(
                refactor=DbtDeprecationRefactor(
                    log=f"Test '{test_name}' - Custom test argument '{field}' moved under 'arguments'.",
                    change_type=ChangeType.TEST_ARGUMENT_MOVED_TO_ARGUMENTS,
                    deprecation=DeprecationType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION,
                    original_location=n.original_location,
                ),
                edited_key_path=[*path_prefix, "arguments", field],
            )
            test_definition["arguments"] = get_dict(test_definition, "arguments")
            assign_node(test_definition["arguments"], field, n)
            self._refactored = True
            self._refactor_entries.append(yr.to_entry())

    def _restructure_yaml_keys_for_node(
        self,
        node: CommentedMap,
        original_node: Optional[CommentedMap],
        node_type: str,
        path_prefix: list,
    ) -> None:
        existing_meta = node.get("meta", CommentedMap()).copy()
        if "meta" in node:
            copy_ca(node["meta"], existing_meta)
        existing_config = node.get("config", {}).copy()
        pretty_node_type = node_type[:-1].title()
        _node_for_loc = original_node if original_node is not None else node
        _config_for_loc = original_node.get("config", {}) if original_node is not None else node.get("config", {})

        for field in existing_config:
            # Special casing target_schema and target_database because they are renamed by another autofix rule
            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields or field in (
                "target_schema",
                "target_database",
            ):
                continue

            self._refactored = True
            if field in COMMON_CONFIG_MISSPELLINGS:
                correct_field = COMMON_CONFIG_MISSPELLINGS[field]
                n = pop_node(node["config"], field, original_parent=_config_for_loc)
                yr = YMLDeprecationRefactor(
                    refactor=DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is a common misspelling of '{correct_field}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                        change_type=ChangeType.CUSTOM_CONFIG_RENAMED_DEPRECATION,
                        original_location=n.original_location,
                    ),
                    edited_key_path=[*path_prefix, "config", correct_field],
                )
                assign_node(node["config"], correct_field, n)
            else:
                n = pop_node(node["config"], field, original_parent=_config_for_loc)
                yr = YMLDeprecationRefactor(
                    refactor=DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is not an allowed config - Moved under config.meta.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                        change_type=ChangeType.CUSTOM_CONFIG_MOVED_TO_META_DEPRECATION,
                        original_location=n.original_location,
                    ),
                    edited_key_path=[*path_prefix, "config", "meta", field],
                )
                node["config"] = get_dict(node, "config")
                node_config_meta = get_dict(node["config"], "meta")
                assign_node(node_config_meta, field, n)
                node["config"]["meta"] = node_config_meta
            self._refactor_entries.append(yr.to_entry())

        # we can not loop node and modify it at the same time
        copy_node = node.copy()
        copy_ca(node, copy_node)

        for field in copy_node:
            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_properties:
                continue
            # This is very hard-coded because it is a 'safe' fix and we don't want to break the user's code
            elif field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
                self._refactored = True
                correct_field = COMMON_PROPERTY_MISSPELLINGS[field.lower()]
                position = list(node.keys()).index(field)
                n = pop_node(node, field, original_parent=_node_for_loc)
                yr = YMLDeprecationRefactor(
                    refactor=DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is a common misspelling of '{correct_field}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                        change_type=ChangeType.PROPERTY_MISSPELLING_DEPRECATION,
                        original_location=n.original_location,
                    ),
                    edited_key_path=[*path_prefix, correct_field],
                )
                assign_node(node, correct_field, n, position=position)
                self._refactor_entries.append(yr.to_entry())
                continue

            if field in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields_without_meta:
                self._refactored = True
                n = pop_node(node, field, original_parent=_node_for_loc)
                node_config = node.get("config", {})
                config_obj = node.setdefault("config", CommentedMap())

                # if the field is not under config, move it under config
                if field not in node_config:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' moved under config.",
                            change_type=ChangeType.NODE_PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                            deprecation=DeprecationType.PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", field],
                    )
                    assign_node(config_obj, field, n)

                # if the field is already under config, it will take precedence there, so we remove it from the top level
                else:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is already under config, it has been removed from the top level.",
                            deprecation=DeprecationType.PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                            change_type=ChangeType.PROPERTY_ALREADY_IN_CONFIG_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", field],
                    )
                    # Preserve existing config value; only restore the top-level comment
                    if n.comments is not None and hasattr(config_obj, "ca"):
                        config_obj.ca.items[field] = n.comments
                self._refactor_entries.append(yr.to_entry())

            if field not in self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields:
                self._refactored = True
                n = pop_node(node, field, original_parent=_node_for_loc)
                closest_match = difflib.get_close_matches(
                    str(field),
                    self.schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields.union(
                        set(self.schema_specs.yaml_specs_per_node_type[node_type].allowed_properties)
                    ),
                    1,
                )
                if closest_match:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not allowed, but '{closest_match[0]}' is. Moved as-is under config.meta but you might want to rename it and move it under config.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                            change_type=ChangeType.CUSTOM_KEY_CLOSEST_MATCH_MOVED_TO_META_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", "meta", field],
                    )
                else:
                    yr = YMLDeprecationRefactor(
                        refactor=DbtDeprecationRefactor(
                            log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not an allowed config - Moved under config.meta.",
                            deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                            change_type=ChangeType.CUSTOM_KEY_MOVED_TO_META_DEPRECATION,
                            original_location=n.original_location,
                        ),
                        edited_key_path=[*path_prefix, "config", "meta", field],
                    )
                node["config"] = get_dict(node, "config")
                node_meta = get_dict(node["config"], "meta")
                assign_node(node_meta, field, n)
                node["config"]["meta"] = node_meta
                self._refactor_entries.append(yr.to_entry())

        if existing_meta:
            self._refactored = True
            meta_node = pop_node(node, "meta", original_parent=_node_for_loc)
            yr = YMLDeprecationRefactor(
                refactor=DbtDeprecationRefactor(
                    log=f"{pretty_node_type} '{node.get('name', '')}' - Moved all the meta fields under config.meta and merged with existing config.meta.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    change_type=ChangeType.META_FIELDS_MERGED_DEPRECATION,
                    original_location=meta_node.original_location,
                ),
                edited_key_path=[*path_prefix, "config", "meta"],
            )

            if "config" not in node:
                node["config"] = CommentedMap()
            if "meta" not in node["config"]:
                node["config"]["meta"] = CommentedMap()
            for key in meta_node.value:
                assign_node(node["config"]["meta"], key, extract_node(meta_node.value, key))
            if meta_node.comments is not None and hasattr(node.get("config", {}), "ca"):
                node["config"].ca.items["meta"] = meta_node.comments
            self._refactor_entries.append(yr.to_entry())


def changeset_replace_non_alpha_underscores_in_name_values(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    return _ReplaceNonAlphaUnderscoresImpl(content, config).execute()


class _ReplaceNonAlphaUnderscoresImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.yml_dict = load_yaml(self.yml_str)
        self._refactor_entries: List[RefactorEntry] = []

    def execute(self) -> YMLRuleRefactorResult:
        self._process()
        refactored = len(self._refactor_entries) > 0
        return YMLRuleRefactorResult(
            rule_name="remove_spaces_in_resource_names",
            refactored=refactored,
            refactored_yaml=DbtYAML().dump_to_string(self.yml_dict) if refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )

    def _process(self) -> None:
        for node_type in self.schema_specs.yaml_specs_per_node_type:
            if node_type in self.yml_dict:
                original_nodes = get_list(self.content.original_parsed, node_type)
                for i, node in enumerate(get_list(self.yml_dict, node_type)):
                    original_node = original_nodes[i] if i < len(original_nodes) else None
                    self._replace_node_name_non_alpha_with_underscores(
                        node, original_node, node_type, path_prefix=[node_type, i]
                    )

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

    def _replace_node_name_non_alpha_with_underscores(
        self,
        node: CommentedMap,
        original_node: Optional[CommentedMap],
        node_type: str,
        path_prefix: list,
    ) -> None:
        pretty_node_type = node_type[:-1].title()
        _node_for_loc = original_node if original_node is not None else node

        name = node.get("name", None)
        if name:
            if node_type == "exposures":
                new_name = self._replace_spaces_outside_jinja(name)
                new_name = self._remove_non_alpha_outside_jinja(new_name)
                deprecation = DeprecationType.EXPOSURE_NAME_DEPRECATION
                change_type = ChangeType.EXPOSURE_NAME_DEPRECATION
            else:
                new_name = self._replace_spaces_outside_jinja(name)
                deprecation = DeprecationType.RESOURCE_NAMES_WITH_SPACES_DEPRECATION
                change_type = ChangeType.RESOURCE_NAME_WITH_SPACES_DEPRECATION

            if new_name and new_name != name:
                original_location = location_of_key(_node_for_loc, "name")
                node["name"] = new_name
                yr = YMLDeprecationRefactor(
                    refactor=DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{name}' - Updated 'name' from '{name}' to '{new_name}'.",
                        change_type=change_type,
                        deprecation=deprecation,
                        original_location=original_location,
                    ),
                    edited_key_path=[*path_prefix, "name"],
                )
                self._refactor_entries.append(yr.to_entry())


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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
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
                        change_type=ChangeType.DUPLICATE_MODEL_REMOVED,
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
    """Remove duplicate keys from a YAML string."""
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
            refactor_entries=[RefactorEntry(refactor=r) for r in self._refactors],
        )

    def _process(self) -> str:
        duplicate_line_numbers: set = set()
        for p in yamllint.linter.run(self.yml_str, yaml_config):
            if p.rule == "key-duplicates":
                self._refactored = True
                duplicate_line_numbers.add(p.line)
                self._refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Found duplicate keys: line {p.line} - {p.desc}",
                        change_type=ChangeType.DUPLICATE_KEY_REMOVED,
                        deprecation=DeprecationType.DUPLICATE_YAML_KEYS_DEPRECATION,
                    )
                )

        if self._refactored:
            clean_str = self._remove_duplicate_key_lines(self.yml_str, duplicate_line_numbers)
            parsed = load_yaml(clean_str)
            return DbtYAML().dump_to_string(parsed)

        return self.yml_str

    @staticmethod
    def _remove_duplicate_key_lines(yml_str: str, duplicate_line_numbers: set) -> str:
        """Remove duplicate key lines (and their indented value blocks) from a YAML string.

        yamllint reports the line number of the second (duplicate) occurrence.
        We remove the FIRST occurrence to keep the last (matching yaml.safe_load behavior).
        """
        lines = yml_str.split("\n")
        lines_to_remove: set = set()

        def remove_block_at(idx: int) -> None:
            """Mark idx and all following more-indented lines for removal."""
            lines_to_remove.add(idx)
            key_indent = len(lines[idx]) - len(lines[idx].lstrip())
            j = idx + 1
            while j < len(lines):
                next_line = lines[j]
                if next_line.strip() == "":
                    lines_to_remove.add(j)
                    j += 1
                    continue
                next_indent = len(next_line) - len(next_line.lstrip())
                if next_indent > key_indent:
                    lines_to_remove.add(j)
                    j += 1
                else:
                    break

        for line_num in sorted(duplicate_line_numbers):
            dup_idx = line_num - 1  # convert to 0-indexed
            if dup_idx >= len(lines):
                continue

            dup_line = lines[dup_idx]
            key_indent = len(dup_line) - len(dup_line.lstrip())

            # Extract key name from the duplicate line
            m = re.match(r"\s*([^:\s][^:]*):", dup_line)
            if not m:
                continue
            key_name = m.group(1).strip()

            # Find the first occurrence at the same indent level within the same mapping
            first_idx = None
            for i in range(dup_idx - 1, -1, -1):
                line = lines[i]
                if not line.strip():
                    continue
                indent = len(line) - len(line.lstrip())
                if indent < key_indent:
                    break
                if indent == key_indent and re.match(r"\s*" + re.escape(key_name) + r"\s*:", line):
                    first_idx = i
                    break

            if first_idx is not None:
                remove_block_at(first_idx)

        return "\n".join(line for i, line in enumerate(lines) if i not in lines_to_remove)
