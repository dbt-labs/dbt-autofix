from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import jinja2
import jinja2.nodes
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore

from dbt_autofix._jinja_environment import get_jinja_environment


@dataclass
class DuplicateConfigKey:
    """A key defined more than once in a config() dictionary literal."""

    key: str
    # Source text of the last occurrence - the one dbt resolves the key to
    kept_value: str
    # Source text of the earlier occurrences, in source order
    dropped_values: List[str]


def statically_parse_unrendered_config(
    string: str,
) -> Tuple[Optional[Dict[str, Any]], List[DuplicateConfigKey]]:
    """Given a string with jinja, extract an unrendered config call along with the keys that
    were defined more than once in a config() dictionary literal.
    If no config call is present, returns (None, []).

    For example, given:
    "{{ config(materialized=env_var('DBT_TEST_STATE_MODIFIED')) }}\nselect 1 as id"
    returns: ({'materialized': "Keyword(key='materialized', value=Call(node=Name(name='env_var', ctx='load'), args=[Const(value='DBT_TEST_STATE_MODIFIED')], kwargs=[], dyn_args=None, dyn_kwargs=None))"}, [])

    No config call:
    "select 1 as id"
    returns: (None, [])

    A repeated dict key resolves to its last occurrence, which is what Jinja and Python do
    when they evaluate the dict literal. Callers that collapse the dictionary into keyword
    arguments can use the returned entries to tell the user which values were dropped.
    """
    # Return early to avoid creating jinja environemt if no config call in input string
    if "config(" not in string:
        return None, []

    env = get_jinja_environment()

    parsed = env.parse(string)
    func_calls = tuple(parsed.find_all(jinja2.nodes.Call))

    config_func_calls = list(
        filter(
            lambda f: hasattr(f, "node") and hasattr(f.node, "name") and f.node.name == "config",
            func_calls,
        )
    )
    # There should only be one {{ config(...) }} call per input
    config_func_call = config_func_calls[0] if config_func_calls else None

    if not config_func_call:
        return None, []

    unrendered_config = {}

    # Handle keyword arguments
    for kwarg in config_func_call.kwargs:
        unrendered_config[kwarg.key] = construct_static_kwarg_value(kwarg, string)

    # Handle dictionary literal arguments (e.g., config({'pre-hook': 'select 1'}))
    # Values are resolved positionally through a cursor that only moves forward, so a
    # repeated key resolves to its last occurrence. jinja2 emits Pair nodes in source order
    # and keeps every duplicate, so this has to stay a sequential walk over arg.items.
    dict_value_cursor = 0
    superseded_values: Dict[str, List[str]] = {}
    for arg in config_func_call.args:
        if isinstance(arg, jinja2.nodes.Dict):
            # Extract key-value pairs from the dictionary
            for pair in arg.items:
                if isinstance(pair.key, jinja2.nodes.Const):
                    key = pair.key.value
                    # Always extract from source to preserve original formatting
                    value_source, dict_value_cursor = _extract_dict_value_from_source(string, key, dict_value_cursor)
                    if key in superseded_values:
                        superseded_values[key].append(unrendered_config[key])
                    else:
                        superseded_values[key] = []
                    unrendered_config[key] = value_source

    duplicate_keys = [
        DuplicateConfigKey(key=key, kept_value=unrendered_config[key], dropped_values=dropped_values)
        for key, dropped_values in superseded_values.items()
        if dropped_values
    ]

    return (unrendered_config if unrendered_config else None), duplicate_keys


def _extract_dict_value_from_source(source_string: str, key: str, search_from: int = 0) -> Tuple[str, int]:
    """Extract a dictionary value from source string.

    This is used for dictionary literal arguments like config({'key': value}).
    Handles both single and double quotes for keys.

    Args:
        source_string: Original source string containing the config macro call
        key: Dictionary key whose value should be extracted
        search_from: Offset to start searching from, so a repeated key resolves to its
            next occurrence rather than always to the first one

    Returns:
        Tuple of the value's source code and the offset just past it. An unresolvable key
        returns ``search_from`` unchanged, so the caller's cursor never moves backwards.
    """
    import re

    # Find the config( and the dictionary
    config_match = re.search(r"\{\{\s*config\s*\(\s*\{", source_string)
    if not config_match:
        return str(key), search_from  # Fallback

    # Try to find the key with both single and double quotes
    # First try with the format as it appears in the source (single quotes by default from repr)
    key_patterns = [
        rf"{re.escape(repr(key))}\s*:\s*",  # 'key': value
        rf'"{re.escape(key)}"\s*:\s*',  # "key": value
    ]

    dict_start = config_match.end()

    resolved = _resolve_dict_value_at_or_after(source_string, key_patterns, max(dict_start, search_from))
    if resolved:
        return resolved

    # The cursor can only sit past a real occurrence of the key if an earlier value was
    # mis-parsed. Re-scan the whole dictionary so that a miss degrades to binding the first
    # occurrence - what this function did before the cursor existed - instead of fabricating a
    # value. Report the cursor unchanged so it can never move backwards.
    if search_from > dict_start:
        resolved = _resolve_dict_value_at_or_after(source_string, key_patterns, dict_start)
        if resolved:
            return resolved[0], search_from

    return repr(key), search_from  # Fallback


def _resolve_dict_value_at_or_after(
    source_string: str, key_patterns: List[str], from_pos: int
) -> Optional[Tuple[str, int]]:
    """Resolve the earliest key-pattern match at or after ``from_pos`` to its value.

    Both quoting styles have to be considered together: taking the first *pattern* that
    matches anywhere would bind a key to a later, differently quoted occurrence and skip the
    one in between.

    Returns:
        Tuple of the value's source code and the offset just past it, or None if no pattern
        matched or the match yielded an empty value.
    """
    import re

    matches = [
        match
        for match in (re.compile(pattern, re.MULTILINE).search(source_string, from_pos) for pattern in key_patterns)
        if match
    ]
    if not matches:
        return None

    key_match = min(matches, key=lambda match: match.start())
    extractor = _SourceCodeExtractor(source_string)
    # Stop at comma or closing brace
    source_value, value_end = extractor.extract_until_delimiter(key_match.end(), delimiters=(",", "}"))
    # Clean up: strip any trailing delimiters that shouldn't be included
    source_value = source_value.rstrip(",}")
    return (source_value, value_end) if source_value else None


class _SourceCodeExtractor:
    """Helper class to extract source code segments while handling nested structures.

    This class encapsulates the logic for parsing source code strings to extract
    values while properly handling:
    - Nested parentheses, brackets, and braces
    - String literals with quotes
    - Escaped characters
    """

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.length = len(source)

    def extract_until_delimiter(self, start_pos: int, delimiters: tuple = (",", ")")) -> Tuple[str, int]:
        """Extract source code from start_pos until a top-level delimiter is found.

        Args:
            start_pos: Position to start extraction from
            delimiters: Tuple of delimiter characters to stop at (when at nesting level 0)

        Returns:
            Tuple of the extracted source code, stripped of leading/trailing whitespace, and
            the offset of the top-level delimiter that ended it - or the end of the source if
            no delimiter was found. Callers scanning for repeated keys use the offset to
            resume past the value they just read.

        Example:
            For "config(key=func(a, b), x)" with start_pos=11 and delimiters=(',', ')'):
            Returns ("func(a, b)", 21)  (stops at the comma after the closing paren)
        """
        paren_count = 0
        bracket_count = 0
        brace_count = 0
        in_string = False
        string_char = None
        end_pos = self.length

        for i in range(start_pos, self.length):
            char = self.source[i]

            # Handle string literals
            if char in ('"', "'") and (i == 0 or self.source[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            # Only process structural characters outside of strings
            elif not in_string:
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                    if paren_count < 0:
                        # Found unmatched closing paren (e.g., end of config())
                        end_pos = i
                        break
                elif char == "[":
                    bracket_count += 1
                elif char == "]":
                    bracket_count -= 1
                elif char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                elif char in delimiters and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                    # Found delimiter at top level
                    end_pos = i
                    break

        return self.source[start_pos:end_pos].strip().rstrip(","), end_pos


def construct_static_kwarg_value(kwarg, source_string: str) -> str:
    """Extract the source code for a kwarg value from the original string.

    This preserves Jinja expressions and original formatting better than str(kwarg),
    which is important for detecting Jinja patterns like env_var() and var().

    Args:
        kwarg: Jinja AST keyword argument node
        source_string: Original source string containing the config macro call

    Returns:
        Source code string for the kwarg value, or str(kwarg) if extraction fails

    Example:
        Input: kwarg with key='materialized', source="config(materialized=env_var('X'))"
        Output: "env_var('X')"
    """
    import re

    try:
        key = kwarg.key

        # Find config( in the string
        config_match = re.search(r"\{\{\s*config\s*\(", source_string)
        if not config_match:
            return str(kwarg)

        # Find the key= pattern after config(
        config_start = config_match.end()
        key_pattern = re.compile(rf"{re.escape(key)}\s*=\s*", re.MULTILINE)
        key_match = key_pattern.search(source_string, config_start)

        if key_match:
            value_start = key_match.end()
            extractor = _SourceCodeExtractor(source_string)
            source_value, _ = extractor.extract_until_delimiter(value_start, delimiters=(",", ")"))

            # Return the extracted source if we got something
            if source_value:
                return source_value
    except Exception:
        pass

    # Fall back to string representation
    return str(kwarg)


@dataclass
class RefArgs:
    name: str
    package: Optional[str]
    version: Optional[str]


def statically_parse_ref(expression: str) -> Optional[RefArgs]:
    """Returns a RefArgs or List[str] object, corresponding to ref or source respectively, given an input jinja expression.

    input: str representing how input node is referenced in tested model sql
        * examples:
        - "ref('my_model_a')"
            -> RefArgs(name='my_model_a', package=None, version=None)
        - "ref('my_model_a', version=3)"
            -> RefArgs(name='my_model_a', package=None, version=3)
        - "ref('package', 'my_model_a', version=3)"
            -> RefArgs(name='my_model_a', package='package', version=3)

    """
    ref: Optional[RefArgs] = None

    try:
        statically_parsed = py_extract_from_source(f"{{{{ {expression} }}}}")
    except ExtractionError:
        pass

    if statically_parsed.get("refs"):
        raw_ref = list(statically_parsed["refs"])[0]
        ref = RefArgs(
            package=raw_ref.get("package"),
            name=raw_ref.get("name"),
            version=raw_ref.get("version"),
        )

    return ref
