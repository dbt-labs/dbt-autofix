"""Vendored Jinja environment setup from dbt-common.

This module contains the minimal subset of dbt-common's Jinja machinery needed
to parse (not render) dbt templates. It exists to avoid pulling in the full
dbt-common transitive dependency tree (agate, protobuf, mashumaro, etc.) for
a parse-only use case.

Vendored from:
  - dbt_common/utils/jinja.py  (name-mangling helpers)
  - dbt_common/clients/jinja.py (parser, environment, extensions, undefined)
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, ClassVar, Dict, List, NoReturn, Optional, Type, Union

import jinja2
import jinja2.ext
import jinja2.nodes
import jinja2.parser
import jinja2.sandbox

# ---------------------------------------------------------------------------
# Constants (from dbt_common/utils/jinja.py)
# https://github.com/dbt-labs/dbt-common/blob/5b331b9c50ca5fee959a9e4fa9ecca964549930c/dbt_common/utils/jinja.py#L6
# ---------------------------------------------------------------------------
MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


# ---------------------------------------------------------------------------
# Name-prefixing helpers (from dbt_common/utils/jinja.py)
# https://github.com/dbt-labs/dbt-common/blob/5b331b9c50ca5fee959a9e4fa9ecca964549930c/dbt_common/utils/jinja.py#L10
# ---------------------------------------------------------------------------
def get_dbt_macro_name(name: str) -> str:
    if name is None:
        raise ValueError("Got None for a macro name!")
    return f"{MACRO_PREFIX}{name}"


def get_materialization_macro_name(
    materialization_name: str,
    adapter_type: Optional[str] = None,
) -> str:
    """Upstream ``with_prefix`` param dropped; always prefixes (the only usage in jinja client code)."""
    if adapter_type is None:
        adapter_type = "default"
    name = f"materialization_{materialization_name}_{adapter_type}"
    return get_dbt_macro_name(name)


def get_docs_macro_name(docs_name: str) -> str:
    """Upstream ``with_prefix`` param dropped; always prefixes (the only usage in jinja client code)."""
    return f"{DOCS_PREFIX}{docs_name}"


def get_test_macro_name(test_name: str) -> str:
    """Upstream ``with_prefix`` param dropped; always prefixes (the only usage in jinja client code)."""
    name = f"test_{test_name}"
    return get_dbt_macro_name(name)


# ---------------------------------------------------------------------------
# MacroType / MacroFuzzParser / MacroFuzzEnvironment
# (from dbt_common/clients/jinja.py)
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class MacroType:
    name: str
    type_params: List[MacroType] = dataclasses.field(default_factory=list)


_ParseReturn = Union[jinja2.nodes.Node, List[jinja2.nodes.Node]]


class MacroFuzzParser(jinja2.parser.Parser):
    def parse_macro(self) -> jinja2.nodes.Macro:
        node = jinja2.nodes.Macro(lineno=next(self.stream).lineno)
        node.name = get_dbt_macro_name(self.parse_assign_target(name_only=True).name)
        self.parse_signature(node)
        node.body = self.parse_statements(("name:endmacro",), drop_needle=True)
        return node

    def parse_signature(self, node: Union[jinja2.nodes.Macro, jinja2.nodes.CallBlock]) -> None:
        setattr(node, "arg_types", [])
        setattr(node, "has_type_annotations", False)

        args = node.args = []  # type: ignore
        defaults = node.defaults = []  # type: ignore

        self.stream.expect("lparen")
        while self.stream.current.type != "rparen":
            if args:
                self.stream.expect("comma")

            arg = self.parse_assign_target(name_only=True)
            arg.set_ctx("param")

            type_name: Optional[str]
            if self.stream.skip_if("colon"):
                node.has_type_annotations = True  # type: ignore
                type_name = self.parse_type_name()
            else:
                type_name = ""

            node.arg_types.append(type_name)  # type: ignore

            if self.stream.skip_if("assign"):
                defaults.append(self.parse_expression())
            elif defaults:
                self.fail("non-default argument follows default argument")

            args.append(arg)
        self.stream.expect("rparen")

    def parse_type_name(self) -> MacroType:
        type_name = self.stream.expect("name").value
        type_ = MacroType(type_name)

        if self.stream.skip_if("lbracket"):
            while self.stream.current.type != "rbracket":
                if type_.type_params:
                    self.stream.expect("comma")
                param_type = self.parse_type_name()
                type_.type_params.append(param_type)

            self.stream.expect("rbracket")

        return type_


class MacroFuzzEnvironment(jinja2.sandbox.SandboxedEnvironment):
    def _parse(self, source: str, name: Optional[str], filename: Optional[str]) -> jinja2.nodes.Template:
        return MacroFuzzParser(self, source, name, filename).parse()


# ---------------------------------------------------------------------------
# Extensions (from dbt_common/clients/jinja.py)
# ---------------------------------------------------------------------------
SUPPORTED_LANG_ARG = jinja2.nodes.Name("supported_languages", "param")


class MaterializationExtension(jinja2.ext.Extension):
    tags: ClassVar[List[str]] = ["materialization"]

    def parse(self, parser: jinja2.parser.Parser) -> _ParseReturn:
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        materialization_name = parser.parse_assign_target(name_only=True).name

        adapter_name = "default"
        node.args = []
        node.defaults = []

        while parser.stream.skip_if("comma"):
            target = parser.parse_assign_target(name_only=True)

            if target.name == "default":
                pass
            elif target.name == "adapter":
                parser.stream.expect("assign")
                value = parser.parse_expression()
                adapter_name = value.value
            elif target.name == "supported_languages":
                target.set_ctx("param")
                node.args.append(target)
                parser.stream.expect("assign")
                languages = parser.parse_expression()
                node.defaults.append(languages)
            else:
                raise ValueError(f"Unexpected argument '{target.name}' to materialization '{materialization_name}'")

        if SUPPORTED_LANG_ARG not in node.args:
            node.args.append(SUPPORTED_LANG_ARG)
            node.defaults.append(jinja2.nodes.List([jinja2.nodes.Const("sql")]))

        node.name = get_materialization_macro_name(materialization_name, adapter_name)
        node.body = parser.parse_statements(("name:endmaterialization",), drop_needle=True)
        return node


class DocumentationExtension(jinja2.ext.Extension):
    tags: ClassVar[List[str]] = ["docs"]

    def parse(self, parser: jinja2.parser.Parser) -> _ParseReturn:
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        docs_name = parser.parse_assign_target(name_only=True).name

        node.args = []
        node.defaults = []
        node.name = get_docs_macro_name(docs_name)
        node.body = parser.parse_statements(("name:enddocs",), drop_needle=True)
        return node


class TestExtension(jinja2.ext.Extension):
    tags: ClassVar[List[str]] = ["test"]

    def parse(self, parser: jinja2.parser.Parser) -> _ParseReturn:
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        test_name = parser.parse_assign_target(name_only=True).name

        parser.parse_signature(node)
        node.name = get_test_macro_name(test_name)
        node.body = parser.parse_statements(("name:endtest",), drop_needle=True)
        return node


# ---------------------------------------------------------------------------
# Undefined handling (from dbt_common/clients/jinja.py)
# ---------------------------------------------------------------------------
def _is_dunder_name(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def create_undefined() -> Type[jinja2.Undefined]:
    class Undefined(jinja2.Undefined):
        def __init__(
            self,
            hint: Optional[str] = None,
            obj: Any = None,
            name: Optional[str] = None,
            exc: Any = None,
        ) -> None:
            super().__init__(hint=hint, name=name)
            self.name = name
            self.hint = hint
            self.unsafe_callable = False
            self.alters_data = False

        def __getitem__(self, name: Any) -> Undefined:
            return self

        def __getattr__(self, name: str) -> Undefined:
            if name == "name" or _is_dunder_name(name):
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
            self.name = name
            return self.__class__(hint=self.hint, name=self.name)

        def __call__(self, *args: Any, **kwargs: Any) -> Undefined:
            return self

        def __reduce__(self) -> NoReturn:
            raise TypeError(f"Compilation Error: undefined variable '{self.name or 'unknown'}'")

    return Undefined


# ---------------------------------------------------------------------------
# Filters (from dbt_common/clients/jinja.py)
# ---------------------------------------------------------------------------
def is_list(value: Any) -> bool:
    return isinstance(value, list)


TEXT_FILTERS: Dict[str, Callable[[Any], Any]] = {
    "as_text": lambda x: x,
    "as_bool": lambda x: x,
    "as_native": lambda x: x,
    "as_number": lambda x: x,
    "is_list": is_list,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_jinja_environment() -> jinja2.Environment:
    """Return a Jinja environment configured for parsing dbt templates.

    This is equivalent to ``get_environment(None, capture_macros=True)``
    from dbt-common but without the dbt-common dependency.
    """
    args: Dict[str, Any] = {
        "extensions": [
            "jinja2.ext.do",
            "jinja2.ext.loopcontrols",
            MaterializationExtension,
            DocumentationExtension,
            TestExtension,
        ],
        "undefined": create_undefined(),
    }

    env = MacroFuzzEnvironment(**args)
    env.filters.update(TEXT_FILTERS)
    return env
