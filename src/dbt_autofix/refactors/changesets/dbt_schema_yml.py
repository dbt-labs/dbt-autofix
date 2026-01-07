import difflib
import re
from typing import List, Tuple, Dict, Any
import yamllint.linter
from copy import deepcopy

from dbt_autofix.refactors.results import YMLRuleRefactorResult
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.retrieve_schemas import SchemaSpecs
from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str, yaml_config
from dbt_autofix.refactors.constants import COMMON_PROPERTY_MISSPELLINGS, COMMON_CONFIG_MISSPELLINGS
from dbt_autofix.refactors.fancy_quotes_utils import FANCY_LEFT_PLACEHOLDER, FANCY_RIGHT_PLACEHOLDER

NUM_SPACES_TO_REPLACE_TAB = 2


def changeset_replace_fancy_quotes(yml_str: str) -> YMLRuleRefactorResult:
    """Replace fancy quotes with appropriate handling based on context.

    Fancy quotes (U+201C ", U+201D ") are handled differently based on their position:
    - Fancy quotes used as YAML delimiters: Replaced with regular quotes
    - Fancy quotes inside string values: Preserved using placeholders (restored later)

    Args:
        yml_str: The YAML string to process

    Returns:
        YMLRuleRefactorResult containing the refactored YAML
    """
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Check if we have any fancy quotes to replace
    has_fancy_quotes = "\u201c" in yml_str or "\u201d" in yml_str

    if not has_fancy_quotes:
        # No fancy quotes to process
        return YMLRuleRefactorResult(
            rule_name="replace_fancy_quotes",
            refactored=False,
            refactored_yaml=yml_str,
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )

    lines = yml_str.split("\n")
    refactored_lines = []
    refactored = False

    for line_num, line in enumerate(lines, 1):
        if "\u201c" not in line and "\u201d" not in line:
            refactored_lines.append(line)
            continue

        # Parse the line to distinguish between fancy quotes as delimiters vs. content
        new_line, line_refactored, inside_string_positions = _process_line_fancy_quotes(line)

        if line_refactored:
            refactored = True
            # Count how many fancy quotes were replaced (not preserved)
            delimiter_count = (line.count("\u201c") + line.count("\u201d")) - len(inside_string_positions)
            if delimiter_count > 0:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Replaced {delimiter_count} fancy quotes with regular quotes on line {line_num}"
                    )
                )

        refactored_lines.append(new_line)

    refactored_yaml = "\n".join(refactored_lines)

    return YMLRuleRefactorResult(
        rule_name="replace_fancy_quotes",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def _process_line_fancy_quotes(line: str) -> Tuple[str, bool, List[int]]:
    """Process a single line to handle fancy quotes based on context.

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
            # else: it's a regular quote inside a fancy-quote-delimited string (shouldn't happen in practice)
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
                    or (string_start_char == '"' and _would_close_string(line, i))
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


def _would_close_string(line: str, pos: int) -> bool:
    """Check if a fancy right quote at position pos would close a string.

    This is a simple heuristic: if there's no regular closing quote after this position,
    then this fancy quote is likely the closing delimiter.
    """
    # Look ahead to see if there's a regular closing quote
    remaining = line[pos + 1 :]
    # If there's no regular quote after, this fancy quote is likely the closer
    return '"' not in remaining


def changeset_owner_properties_yml_str(yml_str: str, schema_specs: SchemaSpecs) -> YMLRuleRefactorResult:
    """Generates a refactored YAML string from a single YAML file
    - moves all the owner fields that are not in owner_properties under config.meta
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for node_type in schema_specs.nodes_with_owner:
        if node_type in yml_dict:
            for i, node in enumerate(yml_dict.get(node_type) or []):
                processed_node, node_refactored, node_refactor_logs = restructure_owner_properties(
                    node, node_type, schema_specs
                )
                if node_refactored:
                    refactored = True
                    yml_dict[node_type][i] = processed_node
                    for log in node_refactor_logs:
                        deprecation_refactors.append(
                            DbtDeprecationRefactor(
                                log=log, deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION
                            )
                        )

    return YMLRuleRefactorResult(
        rule_name="restructure_owner_properties",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def restructure_owner_properties(
    node: Dict[str, Any], node_type: str, schema_specs: SchemaSpecs
) -> Tuple[Dict[str, Any], bool, List[str]]:
    """Restructure owner properties according to dbt conventions.

    Args:
        node: The node dictionary to process
        node_type: The type of node to process
        schema_specs: The schema specifications to use

    Returns:
        Tuple containing:
        - The processed node dictionary
        - Boolean indicating if changes were made
        - List of refactor logs
    """
    refactored = False
    refactor_logs: List[str] = []
    pretty_node_type = node_type[:-1].title()

    if "owner" in node and isinstance(node["owner"], dict):
        owner = node["owner"]
        owner_copy = owner.copy()

        for field in owner_copy:
            if field not in schema_specs.owner_properties:
                refactored = True
                if "config" not in node:
                    node["config"] = {"meta": {}}
                if "meta" not in node["config"]:
                    node["config"]["meta"] = {}
                node["config"]["meta"][field] = owner[field]
                del owner[field]
                refactor_logs.append(
                    f"{pretty_node_type} '{node['name']}' - Owner field '{field}' moved under config.meta."
                )

    return node, refactored, refactor_logs


def changeset_remove_tab_only_lines(yml_str: str) -> YMLRuleRefactorResult:
    """Remove lines that contain only tabs from YAML files.

    Args:
        yml_str: The YAML string to process

    Returns:
        YMLRuleRefactorResult containing the refactored YAML and any changes made
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Process each line
    lines = yml_str.splitlines()
    new_lines = []
    for i, line in enumerate(lines):
        if "\t" in line and line.strip() == "":
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(log=f"Removed line containing only tabs on line {i + 1}")
            )
            new_lines.append("")
        else:
            new_lines.append(line)

    refactored_yaml = "\n".join(new_lines) if refactored else yml_str

    return YMLRuleRefactorResult(
        rule_name="remove_tab_only_lines",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_remove_indentation_version(yml_str: str) -> YMLRuleRefactorResult:
    """Standardizes the format of 'version: 2' in YAML files.

    This function looks for any variations of whitespace around 'version: 2' and
    standardizes them to the format 'version: 2'.

    Args:
        yml_str: The YAML string to process

    Returns:
        YMLRuleRefactorResult containing the refactored YAML and any changes made
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Pattern to match any whitespace around 'version: 2'
    pattern = r"^\s*version\s*:\s*2"
    replacement = "version: 2"

    # Process each line
    lines = yml_str.splitlines()
    for i, line in enumerate(lines):
        if re.match(pattern, line):
            if line != replacement:
                refactored = True
                lines[i] = replacement
                deprecation_refactors.append(
                    DbtDeprecationRefactor(log=f"Removed the extra indentation around 'version: 2' on line {i + 1}")
                )

    refactored_yaml = "\n".join(lines) if refactored else yml_str

    return YMLRuleRefactorResult(
        rule_name="removed_extra_indentation",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_remove_extra_tabs(yml_str: str) -> YMLRuleRefactorResult:
    """Removes extra tabs in the YAML files"""
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    current_yaml = yml_str

    while True:
        found_tab_error = False
        for p in yamllint.linter.run(current_yaml, yaml_config):
            if "found character '\\t' that cannot start any token" in p.desc:
                found_tab_error = True
                refactored = True
                deprecation_refactors.append(
                    DbtDeprecationRefactor(log=f"Found extra tabs: line {p.line} - column {p.column}")
                )
                lines = current_yaml.split("\n")
                if p.line <= len(lines):
                    line = lines[p.line - 1]  # Convert to 0-based index
                    if p.column <= len(line):
                        # Replace tab character with NUM_SPACES_TO_REPLACE_TAB spaces
                        new_line = line[: p.column - 1] + " " * NUM_SPACES_TO_REPLACE_TAB + line[p.column :]
                        lines[p.line - 1] = new_line
                        current_yaml = "\n".join(lines)
                        break  # Exit the yamllint loop to restart with updated content

        if not found_tab_error:
            refactored_yaml = current_yaml
            break

    return YMLRuleRefactorResult(
        rule_name="remove_extra_tabs",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def changeset_refactor_yml_str(yml_str: str, schema_specs: SchemaSpecs) -> YMLRuleRefactorResult:  # noqa: PLR0912,PLR0915
    """Generates a refactored YAML string from a single YAML file
    - moves all the config fields under config
    - moves all the meta fields under config.meta and merges with existing config.meta
    - moves all the unknown fields under config.meta
    - provide some information if some fields don't exist but are similar to allowed fields
    - removes custom top-level keys
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    yml_dict_keys = list(yml_dict.keys())
    for key in yml_dict_keys:
        if key not in schema_specs.valid_top_level_yaml_fields:
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Removed custom top-level key: '{key}'",
                    deprecation=DeprecationType.CUSTOM_TOP_LEVEL_KEY_DEPRECATION,
                )
            )
            yml_dict.pop(key)

    for node_type in schema_specs.yaml_specs_per_node_type:
        if node_type in yml_dict:
            for i, node in enumerate(yml_dict.get(node_type) or []):
                processed_node, node_refactored, node_deprecation_refactors = restructure_yaml_keys_for_node(
                    node, node_type, schema_specs
                )
                if node_refactored:
                    refactored = True
                    yml_dict[node_type][i] = processed_node
                    deprecation_refactors.extend(node_deprecation_refactors)

                if "columns" in processed_node:
                    for column_i, column in enumerate(node["columns"]):
                        processed_column, column_refactored, column_deprecation_refactors = (
                            restructure_yaml_keys_for_node(column, "columns", schema_specs)
                        )
                        if column_refactored:
                            refactored = True
                            yml_dict[node_type][i]["columns"][column_i] = processed_column
                            deprecation_refactors.extend(column_deprecation_refactors)

                        # there might be some tests, but they can be called tests or data_tests
                        some_tests = {"tests", "data_tests"} & set(processed_column)
                        if some_tests:
                            test_key = next(iter(some_tests))
                            for test_i, test in enumerate(node["columns"][column_i][test_key]):
                                processed_test, test_refactored, test_refactor_deprecations = (
                                    restructure_yaml_keys_for_test(test, schema_specs)
                                )
                                if test_refactored:
                                    refactored = True
                                    yml_dict[node_type][i]["columns"][column_i][test_key][test_i] = processed_test
                                    deprecation_refactors.extend(test_refactor_deprecations)

                # if there are tests, we need to restructure them
                some_tests = {"tests", "data_tests"} & set(processed_node)
                if some_tests:
                    test_key = next(iter(some_tests))
                    for test_i, test in enumerate(node[test_key]):
                        processed_test, test_refactored, test_refactor_deprecations = restructure_yaml_keys_for_test(
                            test, schema_specs
                        )
                        if test_refactored:
                            refactored = True
                            yml_dict[node_type][i][test_key][test_i] = processed_test
                            deprecation_refactors.extend(test_refactor_deprecations)

                if "versions" in processed_node:
                    for version_i, version in enumerate(node["versions"]):
                        some_tests = {"tests", "data_tests"} & set(version)
                        if some_tests:
                            test_key = next(iter(some_tests))
                            for test_i, test in enumerate(version[test_key]):
                                processed_test, test_refactored, test_refactor_deprecations = (
                                    restructure_yaml_keys_for_test(test, schema_specs)
                                )
                                if test_refactored:
                                    refactored = True
                                    yml_dict[node_type][i]["versions"][version_i][test_key][test_i] = processed_test
                                    deprecation_refactors.extend(test_refactor_deprecations)

    # for sources, the config can be set at the table level as well, which is one level lower
    if "sources" in yml_dict:
        for i, source in enumerate(yml_dict["sources"]):
            if "tables" in source:
                for j, table in enumerate(source["tables"]):
                    processed_source_table, source_table_refactored, source_table_deprecation_refactors = (
                        restructure_yaml_keys_for_node(table, "tables", schema_specs)
                    )
                    if source_table_refactored:
                        refactored = True
                        yml_dict["sources"][i]["tables"][j] = processed_source_table
                        deprecation_refactors.extend(source_table_deprecation_refactors)

                    some_tests = {"tests", "data_tests"} & set(processed_source_table)
                    if some_tests:
                        test_key = next(iter(some_tests))
                        for test_i, test in enumerate(source["tables"][j][test_key]):
                            processed_test, test_refactored, test_refactor_deprecations = (
                                restructure_yaml_keys_for_test(test, schema_specs)
                            )
                            if test_refactored:
                                refactored = True
                                yml_dict["sources"][i]["tables"][j][test_key][test_i] = processed_test
                                deprecation_refactors.extend(test_refactor_deprecations)

                    if "columns" in processed_source_table:
                        for table_column_i, table_column in enumerate(table["columns"]):
                            processed_table_column, table_column_refactored, table_column_deprecation_refactors = (
                                restructure_yaml_keys_for_node(table_column, "columns", schema_specs)
                            )
                            if table_column_refactored:
                                refactored = True
                                yml_dict["sources"][i]["tables"][j]["columns"][table_column_i] = processed_table_column
                                deprecation_refactors.extend(table_column_deprecation_refactors)

                            some_tests = {"tests", "data_tests"} & set(processed_table_column)
                            if some_tests:
                                test_key = next(iter(some_tests))
                                for test_i, test in enumerate(table_column[test_key]):
                                    processed_test, test_refactored, test_deprecation_refactors = (
                                        restructure_yaml_keys_for_test(test, schema_specs)
                                    )
                                    if test_refactored:
                                        refactored = True
                                        yml_dict["sources"][i]["tables"][j]["columns"][table_column_i][test_key][
                                            test_i
                                        ] = processed_test
                                        deprecation_refactors.extend(test_deprecation_refactors)

    return YMLRuleRefactorResult(
        rule_name="restructure_yaml_keys",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def restructure_yaml_keys_for_test(
    test: Dict[str, Any], schema_specs: SchemaSpecs
) -> Tuple[Dict[str, Any], bool, List[DbtDeprecationRefactor]]:
    """Restructure YAML keys for tests according to dbt conventions.
    Tests are separated from other nodes because
    - they don't support meta
    - they can be either a string or a dict
    - when they are a dict, the top level ist just the test name

    Args:
        test: The test dictionary to process
        schema_specs: The schema specifications to use

    Returns:
        Tuple containing:
        - The processed test dictionary
        - Boolean indicating if changes were made
        - List of refactor logs
    """
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # if the test is a string, we leave it as is
    if isinstance(test, str):
        return test, False, []

    # extract the test name and definition
    test_name = next(iter(test.keys()))
    if isinstance(test[test_name], dict):
        # standard test definition syntax
        test_definition = test[test_name]
    else:
        # alt syntax
        test_name = test["test_name"]
        test_definition = test

    deprecation_refactors.extend(refactor_test_common_misspellings(test_definition, test_name))
    deprecation_refactors.extend(refactor_test_config_fields(test_definition, test_name, schema_specs))
    deprecation_refactors.extend(refactor_test_args(test_definition, test_name))

    return test, len(deprecation_refactors) > 0, deprecation_refactors


def refactor_test_config_fields(
    test_definition: Dict[str, Any], test_name: str, schema_specs: SchemaSpecs
) -> List[DbtDeprecationRefactor]:
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    test_configs = schema_specs.yaml_specs_per_node_type["tests"].allowed_config_fields_without_meta
    test_properties = schema_specs.yaml_specs_per_node_type["tests"].allowed_properties

    copy_test_definition = deepcopy(test_definition)
    for field in copy_test_definition:
        # dbt_utils.mutually_exclusive_ranges accepts partition_by as an argument
        # https://github.com/dbt-labs/dbt-utils/blob/0feb9571187119dc48203ad91d8b064a660d6d3a/macros/generic_tests/mutually_exclusive_ranges.sql#L5
        if field == "partition_by" and test_name == "dbt_utils.mutually_exclusive_ranges":
            continue

        # field is a config and not a property
        if field in test_configs and field not in test_properties:
            node_config = test_definition.get("config", {})

            # if the field is not under config, move it under config
            if field not in node_config:
                node_config.update({field: test_definition[field]})
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Test '{test_name}' - Field '{field}' moved under config.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
                test_definition["config"] = node_config

            # if the field is already under config, overwrite it and remove from top level
            else:
                node_config[field] = test_definition[field]
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Test '{test_name}' - Field '{field}' is already under config, it has been overwritten and removed from the top level.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
                test_definition["config"] = node_config
            del test_definition[field]

    return deprecation_refactors


def refactor_test_common_misspellings(test_definition: Dict[str, Any], test_name: str) -> List[DbtDeprecationRefactor]:
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    for field in test_definition:
        if field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Test '{test_name}' - Field '{field}' is a common misspelling of '{COMMON_PROPERTY_MISSPELLINGS[field.lower()]}', it has been renamed.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                )
            )
            test_definition[COMMON_PROPERTY_MISSPELLINGS[field.lower()]] = test_definition[field]
            del test_definition[field]

    return deprecation_refactors


def refactor_test_args(test_definition: Dict[str, Any], test_name: str) -> List[DbtDeprecationRefactor]:
    """Move non-config args under 'arguments' key
    This refactor is only necessary for custom tests, or tests making use of the alternative test definition syntax ('test_name')
    """
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    copy_test_definition = deepcopy(test_definition)
    # Avoid refactoring if the test already has an arguments key that is not a dict
    if "arguments" in test_definition and not isinstance(test_definition["arguments"], dict):
        return deprecation_refactors

    for field in copy_test_definition:
        # TODO: pull from CustomTestMultiKey on schema_specs once available in jsonschemas
        if field in ("config", "arguments", "test_name", "name", "description", "column_name"):
            continue
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Test '{test_name}' - Custom test argument '{field}' moved under 'arguments'.",
                deprecation=DeprecationType.MISSING_GENERIC_TEST_ARGUMENTS_PROPERTY_DEPRECATION,
            )
        )
        test_definition["arguments"] = test_definition.get("arguments", {})
        test_definition["arguments"].update({field: test_definition[field]})
        del test_definition[field]

    return deprecation_refactors


def restructure_yaml_keys_for_node(
    node: Dict[str, Any], node_type: str, schema_specs: SchemaSpecs
) -> Tuple[Dict[str, Any], bool, List[DbtDeprecationRefactor]]:
    """Restructure YAML keys according to dbt conventions.

    Args:
        node: The node dictionary to process
        node_type: The type of node to process
        schema_specs: The schema specifications to use

    Returns:
        Tuple containing:
        - The processed model dictionary
        - Boolean indicating if changes were made
        - List of refactor logs
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    existing_meta = node.get("meta", {}).copy()
    existing_config = node.get("config", {}).copy()
    pretty_node_type = node_type[:-1].title()

    for field in existing_config:
        # Special casing target_schema and target_database because they are renamed by another autofix rule
        if field in schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields or field in (
            "target_schema",
            "target_database",
        ):
            continue

        refactored = True
        if field in COMMON_CONFIG_MISSPELLINGS:
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is a common misspelling of '{COMMON_CONFIG_MISSPELLINGS[field]}', it has been renamed.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                )
            )
            node["config"][COMMON_CONFIG_MISSPELLINGS[field]] = node["config"][field]
            del node["config"][field]
        else:
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"{pretty_node_type} '{node.get('name', '')}' - Config '{field}' is not an allowed config - Moved under config.meta.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                )
            )
            node_config_meta = node.get("config", {}).get("meta", {})
            node_config_meta.update({field: node["config"][field]})
            node["config"] = node.get("config", {})
            node["config"].update({"meta": node_config_meta})
            del node["config"][field]

    # we can not loop node and modify it at the same time
    copy_node = node.copy()

    for field in copy_node:
        if field in schema_specs.yaml_specs_per_node_type[node_type].allowed_properties:
            continue
        # This is very hard-coded because it is a 'safe' fix and we don't want to break the user's code
        elif field.lower() in COMMON_PROPERTY_MISSPELLINGS.keys():
            refactored = True
            correct_field = COMMON_PROPERTY_MISSPELLINGS[field.lower()]
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is a common misspelling of '{correct_field}', it has been renamed.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                )
            )
            node[correct_field] = node[field]
            del node[field]
            continue

        if field in schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields_without_meta:
            refactored = True
            node_config = node.get("config", {})

            # if the field is not under config, move it under config
            if field not in node_config:
                node_config.update({field: node[field]})
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' moved under config.",
                        deprecation=DeprecationType.PROPERTY_MOVED_TO_CONFIG_DEPRECATION,
                    )
                )
                node["config"] = node_config

            # if the field is already under config, it will take precedence there, so we remove it from the top level
            else:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is already under config, it has been removed from the top level.",
                        deprecation="PropertyMovedToConfigDeprecation",
                    )
                )
            del node[field]

        if field not in schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields:
            refactored = True
            closest_match = difflib.get_close_matches(
                str(field),
                schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields.union(
                    set(schema_specs.yaml_specs_per_node_type[node_type].allowed_properties)
                ),
                1,
            )
            if closest_match:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not allowed, but '{closest_match[0]}' is. Moved as-is under config.meta but you might want to rename it and move it under config.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
            else:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"{pretty_node_type} '{node.get('name', '')}' - Field '{field}' is not an allowed config - Moved under config.meta.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_OBJECT_DEPRECATION,
                    )
                )
            node_meta = node.get("config", {}).get("meta", {})
            node_meta.update({field: node[field]})
            node["config"] = node.get("config", {})
            node["config"].update({"meta": node_meta})
            del node[field]

    if existing_meta:
        refactored = True
        deprecation_refactors.append(
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

    return node, refactored, deprecation_refactors


def changeset_replace_non_alpha_underscores_in_name_values(
    yml_str: str, schema_specs: SchemaSpecs
) -> YMLRuleRefactorResult:
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for node_type in schema_specs.yaml_specs_per_node_type:
        if node_type in yml_dict:
            for i, node in enumerate(yml_dict.get(node_type) or []):
                processed_node, node_deprecation_refactors = replace_node_name_non_alpha_with_underscores(
                    node, node_type
                )
                if node_deprecation_refactors:
                    yml_dict[node_type][i] = processed_node
                    deprecation_refactors.extend(node_deprecation_refactors)

    refactored = len(deprecation_refactors) > 0
    refactored_yaml = DbtYAML().dump_to_string(yml_dict) if refactored else yml_str

    return YMLRuleRefactorResult(
        rule_name="remove_spaces_in_resource_names",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


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


def replace_node_name_non_alpha_with_underscores(node: dict[str, str], node_type: str):
    node_deprecation_refactors: List[DbtDeprecationRefactor] = []
    node_copy = node.copy()
    pretty_node_type = node_type[:-1].title()

    deprecation = None
    name = node.get("name", None)
    new_name = None
    if name:
        if node_type == "exposures":
            new_name = _replace_spaces_outside_jinja(name)
            new_name = _remove_non_alpha_outside_jinja(new_name)
            deprecation = "ExposureNameDeprecation"
        else:
            new_name = _replace_spaces_outside_jinja(name)
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


def changeset_remove_duplicate_models(yml_str: str) -> YMLRuleRefactorResult:
    """Removes duplicate model definitions in YAML files, keeping the last occurrence.

    When the same model name appears multiple times in the models list, this function
    removes all but the last occurrence, aligning with dbt's behavior of keeping the
    last definition when duplicates exist.

    Args:
        yml_str: The YAML string to process

    Returns:
        YMLRuleRefactorResult containing the refactored YAML and any changes made
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    if "models" not in yml_dict or not isinstance(yml_dict["models"], list):
        return YMLRuleRefactorResult(
            rule_name="remove_duplicate_models",
            refactored=False,
            refactored_yaml=yml_str,
            original_yaml=yml_str,
            deprecation_refactors=deprecation_refactors,
        )

    seen_model_names: Dict[str, List[int]] = {}  # Maps model name to list of occurrence indices
    indices_to_remove: List[int] = []

    # First pass: identify all occurrences of each model
    for i, model in enumerate(yml_dict["models"]):
        if not isinstance(model, dict):
            continue

        model_name = model.get("name")
        if model_name is None:
            continue

        if model_name not in seen_model_names:
            seen_model_names[model_name] = []
        seen_model_names[model_name].append(i)

    # Second pass: identify duplicates and mark all but the last occurrence for removal
    for model_name, indices in seen_model_names.items():
        if len(indices) > 1:
            # Found duplicates - mark all but the last occurrence for removal
            refactored = True
            # Remove all indices except the last one (keep the last occurrence)
            indices_to_remove.extend(indices[:-1])
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Model '{model_name}' - Found duplicate definition, removed first occurrence (keeping the second one).",
                    deprecation=DeprecationType.DUPLICATE_YAML_KEYS_DEPRECATION,
                )
            )

    # Third pass: remove duplicates (in reverse order to maintain indices)
    if refactored:
        indices_to_remove.sort(reverse=True)
        for index in indices_to_remove:
            yml_dict["models"].pop(index)

        refactored_yaml = dict_to_yaml_str(yml_dict)
    else:
        refactored_yaml = yml_str

    return YMLRuleRefactorResult(
        rule_name="remove_duplicate_models",
        refactored=refactored,
        refactored_yaml=refactored_yaml,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )
