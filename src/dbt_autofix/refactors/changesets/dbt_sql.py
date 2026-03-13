import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.jinja import statically_parse_unrendered_config
from dbt_autofix.refactors.constants import COMMON_CONFIG_MISSPELLINGS
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    Location,
    SQLContent,
    SQLRefactorConfig,
    SQLRuleRefactorResult,
)

CONFIG_MACRO_PATTERN = re.compile(r"(\{\{\s*config\s*\()(.*?)(\)\s*\}\})", re.DOTALL)


def _find_sql_key_location(content: str, key: str) -> Optional[Location]:
    m = re.search(rf"\b{re.escape(key)}\b", content)
    if not m:
        return None
    prefix = content[: m.start()]
    line = prefix.count("\n") + 1
    line_start = prefix.rfind("\n") + 1
    return Location(line=line, start=m.start() - line_start, end=m.end() - line_start)


def extract_config_macro(sql_content: str) -> Optional[str]:
    """Extract the {{ config(...) }} macro from SQL content.

    This function properly handles nested Jinja expressions like:
    incremental_predicate="data_date = '{{ var('run_date') }}'"

    Args:
        sql_content: The SQL content to search

    Returns:
        The full config macro string, or None if not found
    """
    # Find the start of the config macro
    start_pattern = re.search(r"\{\{\s*config\s*\(", sql_content)
    if not start_pattern:
        return None

    start_pos = start_pattern.start()
    config_start = start_pattern.end()  # Position after "config("

    # Track parentheses balance and string state
    paren_depth = 1  # We're already inside the opening "config("
    in_string = False
    string_char = None
    i = config_start

    while i < len(sql_content) and paren_depth > 0:
        char = sql_content[i]

        # Handle string boundaries
        if char in ('"', "'") and (i == 0 or sql_content[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None

        # Track parentheses only when not in a string
        elif not in_string:
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1

                # If we're back to depth 0, check if this is followed by }}
                if paren_depth == 0:
                    # Look for closing }}
                    remaining = sql_content[i + 1 : i + 10]
                    close_match = re.match(r"\s*\}\}", remaining)
                    if close_match:
                        end_pos = i + 1 + close_match.end()
                        return sql_content[start_pos:end_pos]
                    else:
                        # This ) is not the end of config, continue
                        paren_depth = 1

        i += 1

    # If we get here, we didn't find a proper closing
    return None


def remove_unmatched_endings(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Remove unmatched {% endmacro %} and {% endif %} tags from SQL content.

    Handles:
    - Multi-line tags
    - Whitespace control variants ({%- and -%})
    - Nested blocks
    - Jinja comments ({# ... #})
    - Malformed comments ({#% ... %}, {# ... %#}, {#% ... %#})
    """
    sql_content = content.current_str
    # Regex patterns for Jinja tag and comment matching
    JINJA_TAG_PATTERN = re.compile(r"{%-?\+?\s*((?s:.*?))\s*\+?-?%}", re.DOTALL)
    # Match proper comments {# ... #}
    JINJA_COMMENT_PATTERN = re.compile(r"{#.*?#}", re.DOTALL)
    MACRO_START = re.compile(r"^macro\s+([^\s(]+)")  # Captures macro name
    IF_START = re.compile(r"^if[(\s]+.*")  # if blocks can also be {% if(...) %}
    MACRO_END = re.compile(r"^endmacro")
    IF_END = re.compile(r"^endif")

    # First, identify all comment regions to skip them
    comment_regions: List[Tuple[int, int]] = []
    for comment_match in JINJA_COMMENT_PATTERN.finditer(sql_content):
        comment_regions.append((comment_match.start(), comment_match.end()))

    def is_in_comment(pos: int) -> bool:
        """Check if a position is within a Jinja comment."""
        for start, end in comment_regions:
            if start <= pos < end:
                return True
        return False

    def looks_like_commented_out_code(pos: int) -> bool:
        """Check if a tag at the given position looks like it's part of commented-out code.

        This handles malformed comment syntax like:
        - {#% if ... %} where %} should have been #}
        - Multi-line blocks where the opening has {# but the close tag doesn't

        Strategy: Look backwards from the tag position to find any unclosed {#
        that hasn't been properly closed with #}. This indicates the tag might
        be inside a malformed comment block.
        """
        # Look at content before this position
        content_before = sql_content[:pos]

        # Find all {# openings and #} closings before this position
        # We'll track whether there's an unclosed {#
        comment_depth = 0
        i = 0
        while i < len(content_before):
            if content_before[i : i + 2] == "{#":
                comment_depth += 1
                i += 2
            elif content_before[i : i + 2] == "#}":
                if comment_depth > 0:
                    comment_depth -= 1
                i += 2
            else:
                i += 1

        # If comment_depth > 0, there's an unclosed {# before this position
        # which means this tag might be inside a malformed comment
        return comment_depth > 0

    deprecation_refactors: List[DbtDeprecationRefactor] = []
    # Track macro and if states with their positions
    macro_stack: List[Tuple[int, int, str]] = []  # [(start_pos, end_pos, macro_name), ...]
    if_stack: List[Tuple[int, int]] = []  # [(start_pos, end_pos), ...]

    # Track positions to remove
    to_remove: List[Tuple[int, int]] = []  # [(start_pos, end_pos), ...]

    # Neutralize comment regions so JINJA_TAG_PATTERN can't start matches inside them.
    # Without this, a malformed tag ending like -%#} inside a comment causes the regex
    # to extend past the comment boundary and consume later valid tags.
    clean_content_chars = list(sql_content)
    for start, end in comment_regions:
        for i in range(start, end):
            clean_content_chars[i] = " "
    clean_content = "".join(clean_content_chars)

    # Find all Jinja tags
    for match in JINJA_TAG_PATTERN.finditer(clean_content):
        tag_content = match.group(1)
        start_pos = match.start()
        end_pos = match.end()

        # Skip if this tag is inside a comment (proper or malformed)
        if is_in_comment(start_pos) or looks_like_commented_out_code(start_pos):
            continue

        # Check for macro start
        macro_match = MACRO_START.match(tag_content)
        if macro_match:
            macro_name = macro_match.group(1)
            macro_stack.append((start_pos, end_pos, macro_name))
            continue

        # Check for if start
        if IF_START.match(tag_content):
            if_stack.append((start_pos, end_pos))
            continue

        # Handle endmacro
        if MACRO_END.match(tag_content):
            if not macro_stack:
                to_remove.append((start_pos, end_pos))
                # Count lines, adjusting for content before first newline
                prefix = sql_content[:start_pos]
                first_newline = prefix.find("\n")
                if first_newline == -1:
                    line_num = 1
                else:
                    line_num = prefix.count("\n", first_newline) + 1
                line_start = prefix.rfind("\n") + 1
                start_col = start_pos - line_start
                end_col = end_pos - line_start
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed unmatched {{% endmacro %}} near line {line_num}",
                        deprecation=DeprecationType.UNEXPECTED_JINJA_BLOCK_DEPRECATION,
                        original_location=Location(line=line_num, start=start_col, end=end_col),
                    )
                )
            else:
                macro_stack.pop()
            continue

        # Handle endif
        if IF_END.match(tag_content):
            if not if_stack:
                to_remove.append((start_pos, end_pos))
                # Count lines, adjusting for content before first newline
                prefix = sql_content[:start_pos]
                first_newline = prefix.find("\n")
                if first_newline == -1:
                    line_num = 1
                else:
                    line_num = prefix.count("\n", first_newline) + 1
                line_start = prefix.rfind("\n") + 1
                start_col = start_pos - line_start
                end_col = end_pos - line_start
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed unmatched {{% endif %}} near line {line_num}",
                        deprecation=DeprecationType.UNEXPECTED_JINJA_BLOCK_DEPRECATION,
                        original_location=Location(line=line_num, start=start_col, end=end_col),
                    )
                )
            else:
                if_stack.pop()

    # Remove the unmatched tags from end to start to maintain correct positions
    result = sql_content
    for start, end in sorted(to_remove, reverse=True):
        result = result[:start] + result[end:]

    return SQLRuleRefactorResult(
        rule_name="remove_unmatched_endings",
        refactored=result != sql_content,
        refactored_content=result,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
    )


def refactor_custom_configs_to_meta_sql(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Move custom configs to meta in SQL files."""
    sql_content = content.current_str
    original_content = content.original_str
    schema_specs = config.schema_specs
    node_type = config.node_type
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactor_warnings: list[str] = []

    # Always use static parsing to handle configs with or without Jinja
    config_macro_str = ""
    config_source_map: Dict[str, str] = {}
    original_sql_configs: Dict[str, Any] = {}

    if "config(" in sql_content:
        # Extract the {{ config(...) }} part of sql_content using smart extraction
        # that handles nested Jinja expressions
        config_macro_str = extract_config_macro(sql_content) or ""

        if config_macro_str:
            # Use static parsing to get source code (handles Jinja without rendering)
            original_statically_parsed_config = statically_parse_unrendered_config(config_macro_str) or {}

            if original_statically_parsed_config:
                # Use parsed config values as both data and source map
                original_sql_configs = original_statically_parsed_config
                config_source_map = original_statically_parsed_config.copy()

    if not original_sql_configs:
        # No config found, return early
        return SQLRuleRefactorResult(
            rule_name="move_custom_configs_to_meta_sql",
            refactored=False,
            refactored_content=sql_content,
            original_content=sql_content,
            deprecation_refactors=[],
        )

    refactored_sql_configs = deepcopy(original_sql_configs)

    moved_to_meta = []
    renamed_configs = []

    allowed_config_fields = schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields

    # Special casing snapshots because target_schema and target_database are renamed by another autofix rule
    if node_type == "snapshots":
        allowed_config_fields = allowed_config_fields.union({"target_schema", "target_database"})

    for sql_config_key, sql_config_value in original_sql_configs.items():
        if sql_config_key in COMMON_CONFIG_MISSPELLINGS:
            # Config key is a common misspelling - rename it
            renamed_configs.append(sql_config_key)
            new_key = COMMON_CONFIG_MISSPELLINGS[sql_config_key]
            refactored_sql_configs[new_key] = sql_config_value
            del refactored_sql_configs[sql_config_key]
            # Also update the source map
            if sql_config_key in config_source_map:
                config_source_map[new_key] = config_source_map[sql_config_key]
                del config_source_map[sql_config_key]
        elif sql_config_key not in allowed_config_fields:
            # Config key is not recognized - it's a custom config that should go in meta
            moved_to_meta.append(sql_config_key)

            # Get or create meta dict
            if "meta" not in refactored_sql_configs:
                meta_dict = {}
            else:
                # Meta already exists - parse it if it's a string
                existing_meta = refactored_sql_configs["meta"]
                if isinstance(existing_meta, str):
                    # It's a source code string like "{'key': 'value'}" - parse it
                    import ast

                    try:
                        parsed_meta = ast.literal_eval(existing_meta)
                        meta_dict = {k: repr(v) if not isinstance(v, str) else f"'{v}'" for k, v in parsed_meta.items()}
                    except (ValueError, SyntaxError):
                        # Parsing failed, skip this meta (might contain Jinja)
                        meta_dict = {}
                else:
                    meta_dict = existing_meta

            # Add the custom config to meta
            meta_dict[sql_config_key] = sql_config_value
            refactored_sql_configs["meta"] = meta_dict
            del refactored_sql_configs[sql_config_key]

    # Update {{ config(...) }} macro call with new configs if any were moved to meta or renamed
    refactored_content = None
    refactored = False

    if refactored_sql_configs != original_sql_configs:
        refactored = True

        # Compute refactored content first so we can find edited locations
        new_config_str = _serialize_config_macro_call(refactored_sql_configs, config_source_map)
        old_config = extract_config_macro(sql_content)
        if old_config:
            new_config = f"{{{{ config({new_config_str}\n) }}}}"
            refactored_content = sql_content.replace(old_config, new_config, 1)
        else:
            # Fallback to regex if extraction failed
            def replace_config(match):
                return f"{{{{ config({new_config_str}\n) }}}}"

            refactored_content = CONFIG_MACRO_PATTERN.sub(replace_config, sql_content, count=1)

        # Generate deprecation refactors
        _orig_for_loc = original_content if original_content is not None else sql_content
        for renamed_config in renamed_configs:
            new_name = COMMON_CONFIG_MISSPELLINGS[renamed_config]
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Config '{renamed_config}' is a common misspelling of '{new_name}', it has been renamed.",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                    original_location=_find_sql_key_location(_orig_for_loc, renamed_config),
                    edited_location=_find_sql_key_location(refactored_content, new_name),
                )
            )

        for moved_config in moved_to_meta:
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Moved custom config '{moved_config}' to 'meta'",
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                    original_location=_find_sql_key_location(_orig_for_loc, moved_config),
                    edited_location=_find_sql_key_location(refactored_content, moved_config),
                )
            )

    return SQLRuleRefactorResult(
        rule_name="move_custom_configs_to_meta_sql",
        refactored=refactored,
        refactored_content=refactored_content or sql_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
        refactor_warnings=refactor_warnings,
    )


def _serialize_config_macro_call(config_dict: dict, config_source_map: Optional[Dict[str, str]] = None) -> str:
    """Serialize a config dictionary back to a config macro call string.

    Args:
        config_dict: Dictionary of config keys and values
        config_source_map: Optional dictionary mapping config keys to their original source code strings.
                          Used to preserve Jinja expressions when serializing statically parsed configs.
    """
    if config_source_map is None:
        config_source_map = {}

    if any("-" in k for k in config_dict):
        return str(config_dict)
    else:
        items = []
        for k, v in config_dict.items():
            # If this is the meta key and it's a dict, serialize it specially
            if k == "meta" and isinstance(v, dict):
                meta_items = []
                for meta_k, meta_v in v.items():
                    # Use source map if available for individual meta keys (moved from top-level configs)
                    # This preserves original source code including Jinja expressions
                    if meta_k in config_source_map:
                        meta_v_str = config_source_map[meta_k]
                    elif isinstance(meta_v, str):
                        # Check for AST node string representations
                        if meta_v.startswith(("Keyword", "Call", "Const", "Name", "List")):
                            raise ValueError(
                                f"Failed to extract source code for meta key '{meta_k}'. "
                                f"Got AST representation instead: {meta_v[:100]}... "
                                f"This is a bug in dbt-autofix. Please report this issue with your config() call."
                            )
                        # Add quotes if not already quoted
                        if not (meta_v.startswith('"') or meta_v.startswith("'")):
                            meta_v_str = f"'{meta_v}'"
                        else:
                            meta_v_str = meta_v
                    elif isinstance(meta_v, (dict, list)):
                        # For nested structures, use repr to get proper Python syntax
                        meta_v_str = repr(meta_v)
                    else:
                        meta_v_str = str(meta_v)
                    meta_items.append(f"'{meta_k}': {meta_v_str}")
                v_str = "{" + ", ".join(meta_items) + "}"
            elif k in config_source_map:
                # Preserve original source code exactly — this keeps the user's quoting style
                # and avoids breaking values that contain nested quotes
                v_str = config_source_map[k]
            elif isinstance(v, str):
                # Check if it's already a string representation of an AST node
                # (starts with a class name like "Keyword" or "Call")
                if v.startswith(("Keyword", "Call", "Const", "Name", "List")):
                    # This is an AST node string representation - this should never happen
                    # It indicates that source extraction failed in construct_static_kwarg_value
                    raise ValueError(
                        f"Failed to extract source code for config key '{k}'. "
                        f"Got AST representation instead: {v[:100]}... "
                        f"This is a bug in dbt-autofix. Please report this issue with your config() call."
                    )
                else:
                    # Use double quotes for string values to match expected format
                    v_str = f'"{v}"'
            else:
                v_str = str(v)
            items.append(f"\n    {k}={v_str}")
        return ", ".join(items)


def move_custom_config_access_to_meta_sql(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    """Move custom config access to meta in SQL files."""
    sql_content = content.current_str
    schema_specs = config.schema_specs
    refactored = False
    refactored_content = sql_content
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactor_warnings: List[str] = []

    # Crude way to avoid refactoring the file if it contains any cusotm 'config' variable
    if "set config" in sql_content:
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_sql",
            refactored=False,
            refactored_content=sql_content,
            original_content=sql_content,
            deprecation_refactors=[],
        )

    # Find all instances of config.get(<config-key>, <default>) or config.get(<config-key>)
    pattern = re.compile(r"config\.get\(\s*([\"'])(?P<key>.+?)\1\s*(?:,\s*(?P<default>[^)]+))?\)")
    # To safely replace multiple matches in a string, collect all replacements first,
    # then apply them in reverse order (from end to start) so indices remain valid.
    matches = list(pattern.finditer(refactored_content))
    replacements = []
    allowed_config_fields = set()
    for specs in schema_specs.yaml_specs_per_node_type.values():
        allowed_config_fields.update(specs.allowed_config_fields)

    for match in matches:
        config_key = match.group("key")
        default = match.group("default")

        if config_key in allowed_config_fields:
            continue

        start, end = match.span()
        if default is None:
            replacement = f"config.get('meta').{config_key}"
        else:
            refactor_warnings.append(
                f"Detected config.get({config_key}, {default}) in SQL file, "
                "but autofix was unable to refactor it safely.\n\t"
                "Please manually access the config value from 'meta'.",
            )
            continue
            # replacement = f"(config.get('meta').{config_key} or {default})"
        replacements.append((start, end, replacement, match.group(0)))
        refactored = True

    # Apply replacements in reverse order to avoid messing up indices
    for start, end, replacement, original in reversed(replacements):
        prefix = refactored_content[:start]
        line_num = prefix.count("\n") + 1
        line_start = prefix.rfind("\n") + 1
        col = start - line_start
        refactored_content = refactored_content[:start] + replacement + refactored_content[end:]
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f'Refactored "{original}" to "{replacement}"',
                # Core does not explicitly raise a deprecation for usage of config.get() in SQL files
                deprecation=None,
                original_location=Location(line=line_num, start=col, end=end - line_start),
                edited_location=Location(line=line_num, start=col),
            )
        )

    return SQLRuleRefactorResult(
        rule_name="move_custom_config_access_to_meta_sql",
        refactored=refactored,
        refactored_content=refactored_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
        refactor_warnings=refactor_warnings,
    )


def rename_sql_file_names_with_spaces(content: SQLContent, config: SQLRefactorConfig) -> SQLRuleRefactorResult:
    sql_content = content.current_str
    sql_file_path = content.current_file_path
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    new_file_path = sql_file_path
    if " " in sql_file_path.name:
        new_file_path = sql_file_path.with_name(sql_file_path.name.replace(" ", "_"))
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Renamed '{sql_file_path.name}' to '{new_file_path.name}'",
                deprecation=DeprecationType.RESOURCE_NAMES_WITH_SPACES_DEPRECATION,
            )
        )

    return SQLRuleRefactorResult(
        rule_name="rename_sql_files_with_spaces",
        refactored=sql_file_path != new_file_path,
        refactored_content=sql_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
        refactored_file_path=new_file_path,
    )
