"""Normalize legacy boolean ``static_analysis`` config values to the Fusion enum.

The dbt platform / Fusion engine expects ``static_analysis`` to be one of the string
enum values ``unsafe | off | strict | baseline | on``. Older projects set it to a
boolean (``True``/``False``) or a truthy/falsy spelling (``yes``/``no``), which makes
Fusion fail to load the project manifest with ``dbt1150``:

    unknown variant `False`, expected one of `unsafe`, `off`, `strict`, `baseline`, `on`

This module converts those legacy values to the enum equivalents:

    True  / true / yes  -> 'baseline'
    False / false / no  -> 'off'

Values that are already valid enum members (``on``, ``off``, ``baseline``, ``strict``,
``unsafe``) are left untouched, and so is anything else we don't recognize.
"""

from typing import List, Optional, Tuple

from ruamel.yaml.scalarstring import SingleQuotedScalarString

from dbt_autofix.refactors.results import DbtDeprecationRefactor, YMLContent, YMLRuleRefactorResult
from dbt_autofix.refactors.yml import DbtYAML, load_yaml

STATIC_ANALYSIS_KEY = "static_analysis"
STATIC_ANALYSIS_DEPRECATION = "StaticAnalysisDeprecation"

# Already-valid Fusion enum values - never converted.
VALID_STATIC_ANALYSIS_ENUM = {"unsafe", "off", "strict", "baseline", "on"}

# Legacy spellings that map onto the enum. Note that ``on``/``off`` are intentionally
# absent here: they are already valid enum members and must be preserved as-is.
_TRUTHY_SPELLINGS = {"true", "yes"}
_FALSY_SPELLINGS = {"false", "no"}


def normalize_static_analysis_value(value: object) -> Optional[str]:
    """Return the enum string a ``static_analysis`` value should become, or ``None``.

    ``None`` means the value is already valid (or is something we don't touch) and
    should be left unchanged. Accepts values as parsed from YAML (bool or str).
    """
    if isinstance(value, bool):
        return "baseline" if value else "off"
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUTHY_SPELLINGS:
            return "baseline"
        if lowered in _FALSY_SPELLINGS:
            return "off"
    return None


def normalize_static_analysis_source(source: str) -> Optional[str]:
    """Return the enum string for a ``static_analysis`` value written in a SQL ``config()`` call.

    The value arrives as source code (e.g. ``True``, ``'baseline'``). Returns the bare
    enum string (unquoted) to convert to, or ``None`` to leave the value unchanged.
    """
    stripped = source.strip()
    # Unwrap a quoted string literal so we compare against the inner value.
    if len(stripped) > 1 and stripped[0] in ("'", '"') and stripped[-1] == stripped[0]:
        inner = stripped[1:-1]
    else:
        inner = stripped
    lowered = inner.strip().lower()
    if lowered in _TRUTHY_SPELLINGS:
        return "baseline"
    if lowered in _FALSY_SPELLINGS:
        return "off"
    return None


def _normalize_in_place(node: object, changes: List[Tuple[str, object, str]]) -> None:
    """Recursively walk a parsed YAML structure, normalizing ``static_analysis`` values.

    Mutates ``node`` in place and appends ``(key, old_value, new_value)`` tuples to
    ``changes`` for each conversion performed. Handles both the plain key
    (``static_analysis``, used in schema.yml and at the top level of dbt_project.yml)
    and the ``+``-prefixed key (``+static_analysis``, used under node blocks in
    dbt_project.yml).
    """
    if isinstance(node, dict):
        for key in list(node.keys()):
            value = node[key]
            key_name = key[1:] if isinstance(key, str) and key.startswith("+") else key
            if key_name == STATIC_ANALYSIS_KEY and not isinstance(value, (dict, list)):
                normalized = normalize_static_analysis_value(value)
                if normalized is not None:
                    node[key] = SingleQuotedScalarString(normalized)
                    changes.append((str(key), value, normalized))
                continue
            _normalize_in_place(value, changes)
    elif isinstance(node, list):
        for item in node:
            _normalize_in_place(item, changes)


def changeset_normalize_static_analysis_yml(content: YMLContent, config: object) -> YMLRuleRefactorResult:
    """Normalize boolean ``static_analysis`` config values to the Fusion enum in a YAML file.

    Works for both ``dbt_project.yml`` (top-level and ``+static_analysis`` under node
    blocks) and schema YAML ``config:`` blocks (model/seed/snapshot/source levels).
    """
    yml_str = content.current_str
    yml_dict = load_yaml(yml_str)

    changes: List[Tuple[str, object, str]] = []
    _normalize_in_place(yml_dict, changes)

    deprecation_refactors = [
        DbtDeprecationRefactor(
            log=f"Converted '{key}: {old_value}' to '{key}: {new_value}' (static_analysis must be a Fusion enum value)",
            deprecation=STATIC_ANALYSIS_DEPRECATION,
        )
        for key, old_value, new_value in changes
    ]

    refactored = len(deprecation_refactors) > 0
    return YMLRuleRefactorResult(
        rule_name="normalize_static_analysis",
        refactored=refactored,
        refactored_yaml=DbtYAML().dump_to_string(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )
