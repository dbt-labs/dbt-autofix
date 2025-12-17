import re
from typing import List, Optional, Set, Tuple

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.results import DbtDeprecationRefactor, SQLRuleRefactorResult
from dbt_autofix.retrieve_schemas import SchemaSpecs

# Statically compiled regex patterns for performance
# Pattern to detect config variable shadowing
SET_CONFIG_PATTERN = re.compile(r"{%\s*set\s+config\s*=")
CONFIG_ALIAS_PATTERN = re.compile(r"{%\s*set\s+\w+\s*=\s*config\s*%}")

# Pattern to match config.get() and config.require() calls
# This handles:
# - Single and double quotes
# - Optional whitespace (including multiline)
# - Optional default parameter
# - Optional validator parameter
CONFIG_ACCESS_PATTERN = re.compile(
    r"config\.(get|require)\s*\("  # config.get( or config.require(
    r"(?P<pre_ws>\s*)"  # whitespace before the key
    r"(?P<quote>[\"'])(?P<key>[^\"']+)(?P=quote)"  # quoted key with captured quote style
    r"(?P<rest>.*?)"  # rest of the call including args and whitespace
    r"\)",  # closing paren
    re.DOTALL,
)

# Pattern to detect chained config access
CHAINED_ACCESS_PATTERN = re.compile(
    r"config\.(get|require)\s*\([^)]+\)\s*\."  # config.get(...).
)


def move_custom_config_access_to_meta_sql_improved(
    sql_content: str, schema_specs: SchemaSpecs, node_type: str
) -> SQLRuleRefactorResult:
    """
    Move custom config access to meta in SQL files using the new meta_get/meta_require methods.

    This improved version:
    - Handles both config.get() and config.require()
    - Properly replaces with config.meta_get() and config.meta_require()
    - Handles defaults correctly
    - Preserves validators (now supported in CompileConfig.meta_get())
    - Avoids false positives with better variable detection

    Args:
        sql_content: The SQL content to process
        schema_specs: The schema specifications to use
        node_type: The type of node to process
    """
    refactored = False
    refactored_content = sql_content
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactor_warnings: List[str] = []

    # Check for variable shadowing more carefully
    if SET_CONFIG_PATTERN.search(sql_content) or CONFIG_ALIAS_PATTERN.search(sql_content):
        refactor_warnings.append(
            "Detected potential config variable shadowing. Skipping refactor to avoid false positives."
        )
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_sql_improved",
            refactored=False,
            refactored_content=sql_content,
            original_content=sql_content,
            deprecation_refactors=[],
            refactor_warnings=refactor_warnings,
        )

    # Get all allowed config fields across all node types
    allowed_config_fields: Set[str] = set()
    for specs in schema_specs.yaml_specs_per_node_type.values():
        allowed_config_fields.update(specs.allowed_config_fields)

    # Collect all replacements first
    matches = list(CONFIG_ACCESS_PATTERN.finditer(refactored_content))
    replacements: List[Tuple[int, int, str, str]] = []

    for match in matches:
        method = match.group(1)  # 'get' or 'require'
        pre_whitespace = match.group("pre_ws")  # Whitespace before key
        quote_style = match.group("quote")  # Preserve original quote style
        config_key = match.group("key")
        rest_of_call = match.group("rest")  # Everything after the key including comma, args, and whitespace

        # Skip if this is a dbt-native config
        if config_key in allowed_config_fields:
            continue

        # Build the replacement
        start, end = match.span()
        original = match.group(0)

        # Construct the new method call preserving original formatting
        new_method = f"meta_{method}"

        # Build replacement with preserved whitespace and formatting
        replacement = f"config.{new_method}({pre_whitespace}{quote_style}{config_key}{quote_style}{rest_of_call})"

        replacements.append((start, end, replacement, original))
        refactored = True

    # Apply replacements in reverse order to maintain correct positions
    for start, end, replacement, original in reversed(replacements):
        refactored_content = refactored_content[:start] + replacement + refactored_content[end:]

        # Determine which method was used
        method_used = "get" if ".get(" in original else "require"

        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f'Refactored "{original}" to "{replacement}"',
                # Use the existing deprecation type (assuming it exists)
                # If not, this would need to be added to the DeprecationType enum
                deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
            )
        )

    # Also check for chained access patterns that need manual intervention
    chained_matches = list(CHAINED_ACCESS_PATTERN.finditer(sql_content))
    for match in chained_matches:
        # Extract the config key to check if it's custom
        key_match = re.search(r"([\"'])([^\"']+)\1", match.group(0))
        if key_match and key_match.group(2) not in allowed_config_fields:
            refactor_warnings.append(
                f"Detected chained config access: {match.group(0)[:50]}... "
                "These patterns require manual review as the structure may need to be adjusted."
            )

    return SQLRuleRefactorResult(
        rule_name="move_custom_config_access_to_meta_sql_improved",
        refactored=refactored,
        refactored_content=refactored_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
        refactor_warnings=refactor_warnings,
    )
