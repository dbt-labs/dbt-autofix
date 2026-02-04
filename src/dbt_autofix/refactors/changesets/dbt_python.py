"""Refactoring functions for Python model files in dbt projects.

This module provides functions to:
1. Move custom configs to meta in dbt.config() calls
2. Update dbt.config.get() calls to access custom configs from meta
"""

import ast
import re
from typing import List, Optional, Set, Tuple

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.results import DbtDeprecationRefactor, SQLRuleRefactorResult
from dbt_autofix.retrieve_schemas import SchemaSpecs

# Pattern to find dbt.config(...) calls - captures the full call including parentheses
DBT_CONFIG_CALL_PATTERN = re.compile(
    r"dbt\.config\s*\(",
    re.MULTILINE,
)

# Pattern to find dbt.config.get(...) calls
# Captures: quote style, key name, and optional default value
DBT_CONFIG_GET_PATTERN = re.compile(
    r"dbt\.config\.get\s*\(\s*"  # dbt.config.get(
    r"(?P<quote>[\"'])(?P<key>[^\"']+)(?P=quote)"  # quoted key
    r"(?:\s*,\s*(?P<default>[^)]+))?"  # optional default value
    r"\s*\)",  # closing paren
)


def _find_matching_paren(content: str, start: int) -> int:
    """Find the position of the matching closing parenthesis.

    Args:
        content: The string to search in
        start: Position of the opening parenthesis

    Returns:
        Position of the matching closing parenthesis, or -1 if not found
    """
    depth = 1
    i = start + 1
    in_string = False
    string_char: Optional[str] = None

    while i < len(content) and depth > 0:
        char = content[i]

        # Handle string boundaries
        if char in ('"', "'") and (i == 0 or content[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
        elif not in_string:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1

        i += 1

    return i - 1 if depth == 0 else -1


def _single_to_double_quotes(s: str) -> str:
    """Convert a single-quoted string to double-quoted string.

    Only converts simple string literals like 'value' to "value".
    Leaves other expressions unchanged.

    Args:
        s: A string that may be a single-quoted string literal

    Returns:
        The string with single quotes converted to double quotes if applicable
    """
    # Check if it's a simple single-quoted string (only quotes at start and end)
    if s.startswith("'") and s.endswith("'") and "'" not in s[1:-1]:
        # Extract content and convert to double quotes
        content = s[1:-1]
        # Escape any double quotes in the content
        content = content.replace('"', '\\"')
        return f'"{content}"'
    return s


def _parse_python_kwargs(call_content: str) -> dict[str, str]:
    """Parse keyword arguments from a Python function call.

    Uses AST to safely parse the keyword arguments while preserving
    the original source text for values (to handle complex expressions).

    Args:
        call_content: The content inside the parentheses of a function call

    Returns:
        Dictionary mapping argument names to their source code strings
    """
    # Wrap in a dummy function call to make it valid Python for AST
    dummy_code = f"func({call_content})"

    try:
        tree = ast.parse(dummy_code, mode="eval")
        call_node = tree.body

        if not isinstance(call_node, ast.Call):
            return {}

        result: dict[str, str] = {}
        for keyword in call_node.keywords:
            if keyword.arg is not None:
                # Extract the source code for this value
                # ast.unparse produces single quotes, convert to double quotes
                value_source = ast.unparse(keyword.value)
                value_source = _single_to_double_quotes(value_source)
                result[keyword.arg] = value_source

        return result
    except SyntaxError:
        return {}


def refactor_custom_configs_to_meta_python(
    python_content: str, schema_specs: SchemaSpecs, node_type: str
) -> SQLRuleRefactorResult:
    """Move custom configs to meta in Python dbt.config() calls.

    Transforms:
        dbt.config(materialized="table", random_config="AR")
    To:
        dbt.config(materialized="table", meta={"random_config": "AR"})

    Args:
        python_content: The Python file content to process
        schema_specs: The schema specifications to use for determining allowed configs
        node_type: The type of dbt node (e.g., "models")

    Returns:
        SQLRuleRefactorResult with the refactored content
    """
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Find all dbt.config() calls
    matches = list(DBT_CONFIG_CALL_PATTERN.finditer(python_content))
    if not matches:
        return SQLRuleRefactorResult(
            rule_name="move_custom_configs_to_meta_python",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
        )

    allowed_config_fields = schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields

    # Process matches in reverse order to maintain positions
    refactored_content = python_content
    refactored = False

    for match in reversed(matches):
        # Find the matching closing parenthesis
        open_paren_pos = match.end() - 1  # Position of the (
        close_paren_pos = _find_matching_paren(python_content, open_paren_pos)

        if close_paren_pos == -1:
            continue

        # Extract the content inside the parentheses
        call_content = python_content[open_paren_pos + 1 : close_paren_pos]

        # Parse the kwargs
        kwargs = _parse_python_kwargs(call_content)
        if not kwargs:
            continue

        # Identify custom configs (not in allowed_config_fields)
        custom_configs: dict[str, str] = {}
        native_configs: dict[str, str] = {}
        existing_meta: Optional[str] = None

        for key, value in kwargs.items():
            if key == "meta":
                existing_meta = value
            elif key in allowed_config_fields:
                native_configs[key] = value
            else:
                custom_configs[key] = value

        if not custom_configs:
            continue

        # Build the new meta dict
        meta_items: List[str] = []

        # If there's existing meta, try to parse and merge
        if existing_meta:
            try:
                # Parse the existing meta dict
                existing_meta_parsed = ast.literal_eval(existing_meta)
                if isinstance(existing_meta_parsed, dict):
                    for k, v in existing_meta_parsed.items():
                        meta_items.append(f'"{k}": {v!r}')
            except (ValueError, SyntaxError):
                # Can't parse existing meta, preserve it as-is in the string form
                # This is a fallback - we'll just add to it
                pass

        # Add custom configs to meta
        for key, value in custom_configs.items():
            meta_items.append(f'"{key}": {value}')

        meta_str = "{" + ", ".join(meta_items) + "}"

        # Build the new call content
        new_kwargs: List[str] = []
        for key, value in native_configs.items():
            new_kwargs.append(f"{key}={value}")
        new_kwargs.append(f"meta={meta_str}")

        new_call_content = ", ".join(new_kwargs)
        new_call = f"dbt.config({new_call_content})"

        # Replace in content
        full_match_start = match.start()
        full_match_end = close_paren_pos + 1
        refactored_content = refactored_content[:full_match_start] + new_call + refactored_content[full_match_end:]
        refactored = True

        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Moved custom configs {list(custom_configs.keys())} to 'meta'",
                deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
            )
        )

    return SQLRuleRefactorResult(
        rule_name="move_custom_configs_to_meta_python",
        refactored=refactored,
        refactored_content=refactored_content,
        original_content=python_content,
        deprecation_refactors=deprecation_refactors,
    )


def move_custom_config_access_to_meta_python(
    python_content: str, schema_specs: SchemaSpecs, node_type: str
) -> SQLRuleRefactorResult:
    """Update dbt.config.get() calls to access custom configs from meta.

    Transforms:
        dbt.config.get("random_config")
    To:
        dbt.config.get("meta").get("random_config")

    And with default values:
        dbt.config.get("random_config", "default")
    To:
        dbt.config.get("meta").get("random_config", "default")

    Args:
        python_content: The Python file content to process
        schema_specs: The schema specifications to use for determining allowed configs
        node_type: The type of dbt node (e.g., "models")

    Returns:
        SQLRuleRefactorResult with the refactored content
    """
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    # Get all allowed config fields
    allowed_config_fields: Set[str] = set()
    for specs in schema_specs.yaml_specs_per_node_type.values():
        allowed_config_fields.update(specs.allowed_config_fields)

    # Find all dbt.config.get() calls
    matches = list(DBT_CONFIG_GET_PATTERN.finditer(python_content))
    if not matches:
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_python",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
        )

    # Collect replacements
    replacements: List[Tuple[int, int, str, str]] = []

    for match in matches:
        config_key = match.group("key")
        default_value = match.group("default")

        # Skip if this is a dbt-native config
        if config_key in allowed_config_fields:
            continue

        start, end = match.span()
        original = match.group(0)

        # Build the replacement
        if default_value:
            replacement = f'dbt.config.get("meta").get("{config_key}", {default_value})'
        else:
            replacement = f'dbt.config.get("meta").get("{config_key}")'

        replacements.append((start, end, replacement, original))

    if not replacements:
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_python",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
        )

    # Apply replacements in reverse order
    refactored_content = python_content
    for start, end, replacement, original in reversed(replacements):
        refactored_content = refactored_content[:start] + replacement + refactored_content[end:]

        # Extract the key for the log message
        key_match = re.search(r'["\']([^"\']+)["\']', original)
        key_name = key_match.group(1) if key_match else "unknown"

        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Updated config.get('{key_name}') to config.get('meta').get('{key_name}')",
                deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
            )
        )

    return SQLRuleRefactorResult(
        rule_name="move_custom_config_access_to_meta_python",
        refactored=True,
        refactored_content=refactored_content,
        original_content=python_content,
        deprecation_refactors=deprecation_refactors,
    )
