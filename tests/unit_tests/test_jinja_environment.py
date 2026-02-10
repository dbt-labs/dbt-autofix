"""Tests for the vendored Jinja environment.

Ported from dbt-common: tests/unit/test_jinja.py
(test_macro_parser_parses_simple_types, test_macro_parser_parses_complex_types)
"""

import jinja2
from jinja2 import select_autoescape

from dbt_autofix._jinja_environment import MacroFuzzParser, MacroType


def test_macro_parser_parses_simple_types() -> None:
    macro_txt = """
    {% macro test_macro(param1: str, param2: int, param3: bool, param4: float, param5: Any) %}
    {% endmacro %}
    """

    env = jinja2.Environment(autoescape=select_autoescape())
    parser = MacroFuzzParser(env, macro_txt)
    result = parser.parse()
    arg_types = result.body[1].arg_types
    assert arg_types[0] == MacroType("str")
    assert arg_types[1] == MacroType("int")
    assert arg_types[2] == MacroType("bool")
    assert arg_types[3] == MacroType("float")
    assert arg_types[4] == MacroType("Any")


def test_macro_parser_parses_complex_types() -> None:
    macro_txt = """
    {% macro test_macro(param1: List[str], param2: Dict[ int,str ], param3: Optional[List[str]], param4: Dict[str, Dict[bool, Any]]) %}
    {% endmacro %}
    """

    env = jinja2.Environment(autoescape=select_autoescape())
    parser = MacroFuzzParser(env, macro_txt)
    result = parser.parse()
    arg_types = result.body[1].arg_types
    assert arg_types[0] == MacroType("List", [MacroType("str")])
    assert arg_types[1] == MacroType("Dict", [MacroType("int"), MacroType("str")])
    assert arg_types[2] == MacroType("Optional", [MacroType("List", [MacroType("str")])])
    assert arg_types[3] == MacroType(
        "Dict", [MacroType("str"), MacroType("Dict", [MacroType("bool"), MacroType("Any")])]
    )
