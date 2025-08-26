import re
from typing import List, Tuple

from dbt_autofix.refactors.results import SQLRuleRefactorResult
from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.jinja import statically_parse_unrendered_config
from dbt_autofix.refactors.constants import COMMON_CONFIG_MISSPELLINGS
from dbt_common.clients.jinja import get_template, render_template
from dbt_autofix.retrieve_schemas import SchemaSpecs
from copy import deepcopy
from pathlib import Path


CONFIG_MACRO_PATTERN = re.compile(r"(\{\{\s*config\s*\()(.*?)(\)\s*\}\})", re.DOTALL)


def remove_unmatched_endings(sql_content: str) -> SQLRuleRefactorResult:  # noqa: PLR0912
    """Remove unmatched {% endmacro %} and {% endif %} tags from SQL content.

    Handles:
    - Multi-line tags
    - Whitespace control variants ({%- and -%})
    - Nested blocks

    Args:
        sql_content: The SQL content to process

    Returns: SQLRuleRefactorResult
    """
    # Regex patterns for Jinja tag matching
    JINJA_TAG_PATTERN = re.compile(r"{%-?\s*((?s:.*?))\s*-?%}", re.DOTALL)
    MACRO_START = re.compile(r"^macro\s+([^\s(]+)")  # Captures macro name
    IF_START = re.compile(r"^if[(\s]+.*")  # if blocks can also be {% if(...) %}
    MACRO_END = re.compile(r"^endmacro")
    IF_END = re.compile(r"^endif")

    deprecation_refactors: List[DbtDeprecationRefactor] = []
    # Track macro and if states with their positions
    macro_stack: List[Tuple[int, int, str]] = []  # [(start_pos, end_pos, macro_name), ...]
    if_stack: List[Tuple[int, int]] = []  # [(start_pos, end_pos), ...]

    # Track positions to remove
    to_remove: List[Tuple[int, int]] = []  # [(start_pos, end_pos), ...]

    # Find all Jinja tags
    for match in JINJA_TAG_PATTERN.finditer(sql_content):
        tag_content = match.group(1)
        start_pos = match.start()
        end_pos = match.end()

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
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed unmatched {{% endmacro %}} near line {line_num}",
                        deprecation=DeprecationType.UNEXPECTED_JINJA_BLOCK_DEPRECATION
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
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Removed unmatched {{% endif %}} near line {line_num}",
                        deprecation=DeprecationType.UNEXPECTED_JINJA_BLOCK_DEPRECATION
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


def refactor_custom_configs_to_meta_sql(sql_content: str, schema_specs: SchemaSpecs, node_type: str) -> SQLRuleRefactorResult:
    """Move custom configs to meta in SQL files.

    Args:
        sql_content: The SQL content to process
        schema_specs: The schema specifications to use
        node_type: The type of node to process
    """
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactor_warnings: list[str] = []

    # Capture original config macro calls
    original_sql_configs = {}
    def capture_config(*args, **kwargs):
        if args and len(args) > 0:
            original_sql_configs.update(args[0])
        original_sql_configs.update(kwargs)

    ctx = {"config": capture_config}

    original_statically_parsed_config = {}
    refactored_sql_configs = None
    contains_jinja = False
    try:
        # Crude way to avoid rendering the template if it doesn't contain 'config'
        if "config(" in sql_content:
            # Use regex to extract the {{ config(...) }} part of sql_content
            config_macro_match = CONFIG_MACRO_PATTERN.search(sql_content)
            config_macro_str = config_macro_match.group(0) if config_macro_match else ""

            original_statically_parsed_config = statically_parse_unrendered_config(config_macro_str) or {}

            template = get_template(config_macro_str, ctx=ctx, capture_macros=True)
            render_template(template, ctx=ctx)

        refactored_sql_configs = deepcopy(original_sql_configs)
    except Exception:
        # config() macro calls with jinja requiring dbt context will result in deepcopy error due to Undefined values
        refactored_sql_configs = None
        contains_jinja = True

        if original_statically_parsed_config:
            original_sql_configs = original_statically_parsed_config

    moved_to_meta = []
    renamed_configs = []
    for sql_config_key, sql_config_value in original_sql_configs.items():
        # If the config key is not in the schema specs, move it to meta
        allowed_config_fields = schema_specs.yaml_specs_per_node_type[node_type].allowed_config_fields
    
        # Special casing snapshots because target_schema and target_database are renamed by another autofix rule
        if node_type == "snapshots":
            allowed_config_fields = allowed_config_fields.union({"target_schema", "target_database"})

        if sql_config_key in COMMON_CONFIG_MISSPELLINGS:
            renamed_configs.append(sql_config_key)
            if refactored_sql_configs is not None:
                refactored_sql_configs[COMMON_CONFIG_MISSPELLINGS[sql_config_key]] = sql_config_value
                del refactored_sql_configs[sql_config_key]
        elif sql_config_key not in allowed_config_fields:
            moved_to_meta.append(sql_config_key)
            if refactored_sql_configs is not None:
                if "meta" not in refactored_sql_configs:
                    refactored_sql_configs["meta"] = {}
                refactored_sql_configs["meta"].update({sql_config_key: sql_config_value})
                del refactored_sql_configs[sql_config_key]

    # Update {{ config(...) }} macro call with new configs if any were moved to meta or renamed
    refactored_content = None
    if refactored_sql_configs and refactored_sql_configs != original_sql_configs:
        # Determine if jinja rendering occurred as part of config macro call
        original_config_str = _serialize_config_macro_call(original_sql_configs)
        post_render_statically_parsed_config = statically_parse_unrendered_config(f"{{{{ config({original_config_str}) }}}}")
        if post_render_statically_parsed_config == original_statically_parsed_config:
            refactored = True
            for renamed_config in renamed_configs:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Config '{renamed_config}' is a common misspelling of '{COMMON_CONFIG_MISSPELLINGS[renamed_config]}', it has been renamed.",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
                    )
                )
            if moved_to_meta:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=f"Moved custom config{'s' if len(moved_to_meta) > 1 else ''} {moved_to_meta} to 'meta'",
                        deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION
                    )
                )
            new_config_str = _serialize_config_macro_call(refactored_sql_configs)
            def replace_config(match):
                return f"{{{{ config({new_config_str}\n) }}}}"
            refactored_content = CONFIG_MACRO_PATTERN.sub(replace_config, sql_content, count=1)
        else:
            contains_jinja = True
    
    if contains_jinja:
        if moved_to_meta:
            refactor_warnings.append(
                f"Detected custom config{'s' if len(moved_to_meta) > 1 else ''} {moved_to_meta}, "
                f"but autofix was unable to refactor {'them' if len(moved_to_meta) > 1 else 'it'} due to Jinja usage in the config macro call.\n\t"
                f"Please manually move the custom configs {moved_to_meta} to 'meta'.",
            )
        if renamed_configs:
            refactor_warnings.append(
                f"Detected incorrect spelling of config{'s' if len(renamed_configs) > 1 else ''} {renamed_configs}, "
                f"but autofix was unable to refactor {'them' if len(renamed_configs) > 1 else 'it'} due to Jinja usage in the config macro call.\n\t"
                f"Please manually rename the custom configs {renamed_configs} to the correct spelling.",
            )

    return SQLRuleRefactorResult(
        rule_name="move_custom_configs_to_meta_sql",
        refactored=refactored,
        refactored_content=refactored_content or sql_content,
        original_content=sql_content,
        deprecation_refactors=deprecation_refactors,
        refactor_warnings=refactor_warnings,
    )


def _serialize_config_macro_call(config_dict: dict) -> str:
    if any('-' in k for k in config_dict):
       return str(config_dict) 
    else:
        items = []
        for k, v in config_dict.items():
            if isinstance(v, str):
                v_str = f'"{v}"'
            else:
                v_str = str(v)
            items.append(f"\n    {k}={v_str}")
        return ", ".join(items)


def move_custom_config_access_to_meta_sql(sql_content: str, schema_specs: SchemaSpecs, node_type: str) -> SQLRuleRefactorResult:
    """Move custom config access to meta in SQL files.

    Args:
        sql_content: The SQL content to process
        schema_specs: The schema specifications to use
        node_type: The type of node to process
    """
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
    pattern = re.compile(
        r"config\.get\(\s*([\"'])(?P<key>.+?)\1\s*(?:,\s*(?P<default>[^)]+))?\)"
    )
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
        refactored_content = refactored_content[:start] + replacement + refactored_content[end:]
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f'Refactored "{original}" to "{replacement}"',
                # Core does not explicitly raise a deprecation for usage of config.get() in SQL files
                deprecation=None
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


def rename_sql_file_names_with_spaces(sql_content: str, sql_file_path: Path):
    deprecation_refactors: List[DbtDeprecationRefactor] = []

    new_file_path = sql_file_path
    if " " in sql_file_path.name:
        new_file_path = sql_file_path.with_name(sql_file_path.name.replace(" ", "_"))
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log=f"Renamed '{sql_file_path.name}' to '{new_file_path.name}'",
                deprecation=DeprecationType.RESOURCE_NAMES_WITH_SPACES_DEPRECATION
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