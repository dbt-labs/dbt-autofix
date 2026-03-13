import re
from typing import List, Set, Tuple

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    Location,
    SQLContent,
    SQLRefactorConfig,
    SQLRuleRefactorResult,
)

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
    r"(model.)?config\.(get|require)\s*\("  # config.get( or config.require(
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
    content: SQLContent, config: SQLRefactorConfig
) -> SQLRuleRefactorResult:
    """Move custom config access to meta in SQL files using the new meta_get/meta_require methods.

    This improved version:
    - Handles both config.get() and config.require()
    - Properly replaces with config.meta_get() and config.meta_require()
    - Handles defaults correctly
    - Preserves validators (now supported in CompileConfig.meta_get())
    - Avoids false positives with better variable detection
    """
    return _MoveCustomConfigAccessToMetaSqlImprovedImpl(content, config).execute()


class _MoveCustomConfigAccessToMetaSqlImprovedImpl:
    def __init__(self, content: SQLContent, config: SQLRefactorConfig) -> None:
        self.sql_str = content.current_str
        self.original_str = content.original_str
        self.config = config
        self.schema_specs = config.schema_specs
        self.node_type = config.node_type
        self._deprecation_refactors: list[DbtDeprecationRefactor] = []

    def execute(self) -> SQLRuleRefactorResult:
        sql_content = self.sql_str
        refactored = False
        refactored_content = sql_content
        refactor_warnings: List[str] = []

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

        allowed_config_fields: Set[str] = set()
        for specs in self.schema_specs.yaml_specs_per_node_type.values():
            allowed_config_fields.update(specs.allowed_config_fields)

        orig_relevant = [
            m for m in CONFIG_ACCESS_PATTERN.finditer(self.original_str) if m.group("key") not in allowed_config_fields
        ]

        matches = list(CONFIG_ACCESS_PATTERN.finditer(refactored_content))
        replacements: List[Tuple[int, int, str, str, Location]] = []

        orig_idx = 0
        for match in matches:
            method = match.group(2)  # 'get' or 'require'
            pre_whitespace = match.group("pre_ws")
            quote_style = match.group("quote")
            config_key = match.group("key")
            rest_of_call = match.group("rest")

            if config_key in allowed_config_fields:
                continue

            start, end = match.span()
            original = match.group(0)

            new_method = f"meta_{method}"
            replacement = f"config.{new_method}({pre_whitespace}{quote_style}{config_key}{quote_style}{rest_of_call})"

            orig_m = orig_relevant[orig_idx]
            orig_idx += 1
            orig_prefix = self.original_str[: orig_m.start()]
            line_num = orig_prefix.count("\n") + 1
            orig_ls = orig_prefix.rfind("\n") + 1
            col = orig_m.start() - orig_ls
            end_col = orig_m.end() - orig_ls

            replacements.append((start, end, replacement, original, Location(line=line_num, start=col, end=end_col)))
            refactored = True

        for start, end, replacement, original, location in reversed(replacements):
            prefix = refactored_content[:start]
            edit_line = prefix.count("\n") + 1
            edit_ls = prefix.rfind("\n") + 1
            edit_col = start - edit_ls
            refactored_content = refactored_content[:start] + replacement + refactored_content[end:]

            self._deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f'Refactored "{original}" to "{replacement}"',
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                    original_location=location,
                    edited_location=Location(line=edit_line, start=edit_col, end=edit_col + len(replacement)),
                )
            )

        chained_matches = list(CHAINED_ACCESS_PATTERN.finditer(sql_content))
        for match in chained_matches:
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
            deprecation_refactors=self._deprecation_refactors,
            refactor_warnings=refactor_warnings,
        )
