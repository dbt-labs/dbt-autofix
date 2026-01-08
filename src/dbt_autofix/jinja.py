from dataclasses import dataclass, field
import linecache
import codecs
import os
import tempfile

from types import CodeType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
    Type,
    NoReturn,
)
from typing_extensions import Protocol

import jinja2
import jinja2.ext
import jinja2.parser
import jinja2.nodes
import jinja2.sandbox

from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore


# this is ported from dbt-common/utils/jinja.py

MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


def get_dbt_macro_name(name: str) -> str:
    if name is None:
        raise DbtInternalError("Got None for a macro name!")
    return f"{MACRO_PREFIX}{name}"


def get_dbt_docs_name(name: str) -> str:
    if name is None:
        raise DbtInternalError("Got None for a doc name!")
    return f"{DOCS_PREFIX}{name}"


def get_materialization_macro_name(
    materialization_name: str, adapter_type: Optional[str] = None, with_prefix: bool = True
) -> str:
    if adapter_type is None:
        adapter_type = "default"
    name = f"materialization_{materialization_name}_{adapter_type}"
    return get_dbt_macro_name(name) if with_prefix else name


def get_docs_macro_name(docs_name: str, with_prefix: bool = True) -> str:
    return get_dbt_docs_name(docs_name) if with_prefix else docs_name


def get_test_macro_name(test_name: str, with_prefix: bool = True) -> str:
    name = f"test_{test_name}"
    return get_dbt_macro_name(name) if with_prefix else name


# this is ported from dbt-common/clients/jinja.py
SUPPORTED_LANG_ARG = jinja2.nodes.Name("supported_languages", "param")

# Global which can be set by dependents of dbt-common (e.g. core via flag parsing)
MACRO_DEBUGGING: Union[str, bool] = False

_ParseReturn = Union[jinja2.nodes.Node, List[jinja2.nodes.Node]]


# Temporary type capturing the concept the functions in this file expect for a "node"
class _NodeProtocol(Protocol):
    pass


class MaterializationExtension(jinja2.ext.Extension):
    tags = set(["materialization"])

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
                adapter_name = value.value  # type: ignore

            elif target.name == "supported_languages":
                target.set_ctx("param")
                node.args.append(target)
                parser.stream.expect("assign")
                languages = parser.parse_expression()
                node.defaults.append(languages)

            else:
                raise Exception(materialization_name, target.name)

        if SUPPORTED_LANG_ARG not in node.args:
            node.args.append(SUPPORTED_LANG_ARG)
            node.defaults.append(jinja2.nodes.List([jinja2.nodes.Const("sql")]))

        node.name = get_materialization_macro_name(materialization_name, adapter_name)

        node.body = parser.parse_statements(("name:endmaterialization",), drop_needle=True)

        return node


class DocumentationExtension(jinja2.ext.Extension):
    tags = set(["docs"])

    def parse(self, parser: jinja2.parser.Parser) -> _ParseReturn:
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        docs_name = parser.parse_assign_target(name_only=True).name

        node.args = []
        node.defaults = []
        node.name = get_docs_macro_name(docs_name)
        node.body = parser.parse_statements(("name:enddocs",), drop_needle=True)
        return node


class TestExtension(jinja2.ext.Extension):
    tags = set(["test"])

    def parse(self, parser: jinja2.parser.Parser) -> _ParseReturn:
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        test_name = parser.parse_assign_target(name_only=True).name

        parser.parse_signature(node)
        node.name = get_test_macro_name(test_name)
        node.body = parser.parse_statements(("name:endtest",), drop_needle=True)
        return node


def _linecache_inject(source: str, write: bool) -> str:
    if write:
        # this is the only reliable way to accomplish this. Obviously, it's
        # really darn noisy and will fill your temporary directory
        tmp_file = tempfile.NamedTemporaryFile(
            prefix="dbt-macro-compiled-",
            suffix=".py",
            delete=False,
            mode="w+",
            encoding="utf-8",
        )
        tmp_file.write(source)
        filename = tmp_file.name
    else:
        # `codecs.encode` actually takes a `bytes` as the first argument if
        # the second argument is 'hex' - mypy does not know this.
        rnd = codecs.encode(os.urandom(12), "hex")
        filename = rnd.decode("ascii")

    # put ourselves in the cache
    cache_entry = (len(source), None, [line + "\n" for line in source.splitlines()], filename)
    # linecache does in fact have an attribute `cache`, thanks
    linecache.cache[filename] = cache_entry
    return filename


@dataclass
class MacroType:
    name: str
    type_params: List["MacroType"] = field(default_factory=list)


class MacroFuzzParser(jinja2.parser.Parser):
    def parse_macro(self) -> jinja2.nodes.Macro:
        node = jinja2.nodes.Macro(lineno=next(self.stream).lineno)

        # modified to fuzz macros defined in the same file. this way
        # dbt can understand the stack of macros being called.
        #  - @cmcarthur
        node.name = get_dbt_macro_name(self.parse_assign_target(name_only=True).name)

        self.parse_signature(node)
        node.body = self.parse_statements(("name:endmacro",), drop_needle=True)
        return node

    def parse_signature(self, node: Union[jinja2.nodes.Macro, jinja2.nodes.CallBlock]) -> None:
        """Overrides the default jinja Parser.parse_signature method, modifying
        the original implementation to allow macros to have typed parameters."""

        # Jinja does not support extending its node types, such as Macro, so
        # at least while typed macros are experimental, we will patch the
        # information onto the existing types.
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
                type_name = self.parse_type_name()  # type: ignore
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
        # NOTE: Types syntax is validated here, but not whether type names
        # are valid or have correct parameters.

        # A type name should consist of a name (i.e. 'Dict')...
        type_name = self.stream.expect("name").value
        type = MacroType(type_name)

        # ..and an optional comma-delimited list of type parameters
        # as in the type declaration 'Dict[str, str]'
        if self.stream.skip_if("lbracket"):
            while self.stream.current.type != "rbracket":
                if type.type_params:
                    self.stream.expect("comma")
                param_type = self.parse_type_name()
                type.type_params.append(param_type)

            self.stream.expect("rbracket")

        return type


class MacroFuzzEnvironment(jinja2.sandbox.SandboxedEnvironment):
    def _parse(self, source: str, name: Optional[str], filename: Optional[str]) -> jinja2.nodes.Template:
        return MacroFuzzParser(self, source, name, filename).parse()

    def _compile(self, source: str, filename: str) -> CodeType:
        """
        Override jinja's compilation. Use to stash the rendered source inside
        the python linecache for debugging when the appropriate environment
        variable is set.

        If the value is 'write', also write the files to disk.
        WARNING: This can write a ton of data if you aren't careful.
        """
        if filename == "<template>" and MACRO_DEBUGGING:
            write = MACRO_DEBUGGING == "write"
            filename = _linecache_inject(source, write)

        return super()._compile(source, filename)  # type: ignore


def _is_dunder_name(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def create_undefined(node: Optional[_NodeProtocol] = None) -> Type[jinja2.Undefined]:
    class Undefined(jinja2.Undefined):
        def __init__(
            self,
            hint: Optional[str] = None,
            obj: Any = None,
            name: Optional[str] = None,
            exc: Any = None,
        ) -> None:
            super().__init__(hint=hint, name=name)
            self.node = node
            self.name = name
            self.hint = hint
            # jinja uses these for safety, so we have to override them.
            # see https://github.com/pallets/jinja/blob/master/jinja2/sandbox.py#L332-L339 # noqa
            self.unsafe_callable = False
            self.alters_data = False

        def __getitem__(self, name: Any) -> "Undefined":
            # Propagate the undefined value if a caller accesses this as if it
            # were a dictionary
            return self

        def __getattr__(self, name: str) -> "Undefined":
            if name == "name" or _is_dunder_name(name):
                raise AttributeError("'{}' object has no attribute '{}'".format(type(self).__name__, name))

            self.name = name

            return self.__class__(hint=self.hint, name=self.name)

        def __call__(self, *args: Any, **kwargs: Any) -> "Undefined":
            return self

        def __reduce__(self) -> NoReturn:
            raise Exception(f"name={self.name or 'unknown'}, node={node}")

    return Undefined


def is_list(value):
    return isinstance(value, list)


TEXT_FILTERS: Dict[str, Callable[[Any], Any]] = {
    "as_text": lambda x: x,
    "as_bool": lambda x: x,
    "as_native": lambda x: x,
    "as_number": lambda x: x,
    "is_list": is_list,
}


def get_environment(
    node: Optional[_NodeProtocol] = None,  # always none in autofix
    capture_macros: bool = False,
) -> jinja2.Environment:
    args: Dict[str, List[Union[str, Type[jinja2.ext.Extension]]]] = {
        "extensions": ["jinja2.ext.do", "jinja2.ext.loopcontrols"]
    }

    if capture_macros:
        args["undefined"] = create_undefined(None)  # type: ignore

    args["extensions"].append(MaterializationExtension)
    args["extensions"].append(DocumentationExtension)
    args["extensions"].append(TestExtension)

    env_cls: Type[jinja2.Environment] = MacroFuzzEnvironment
    filters = TEXT_FILTERS

    env = env_cls(**args)
    env.filters.update(filters)

    return env


def statically_parse_unrendered_config(string: str) -> Optional[Dict[str, Any]]:
    """
    Given a string with jinja, extract an unrendered config call.
    If no config call is present, returns None.

    For example, given:
    "{{ config(materialized=env_var('DBT_TEST_STATE_MODIFIED')) }}\nselect 1 as id"
    returns: {'materialized': "Keyword(key='materialized', value=Call(node=Name(name='env_var', ctx='load'), args=[Const(value='DBT_TEST_STATE_MODIFIED')], kwargs=[], dyn_args=None, dyn_kwargs=None))"}

    No config call:
    "select 1 as id"
    returns: None
    """
    # Return early to avoid creating jinja environemt if no config call in input string
    if "config(" not in string:
        return None

    # set 'capture_macros' to capture undefined
    env = get_environment(node=None, capture_macros=True)

    parsed = env.parse(string)
    func_calls = tuple(parsed.find_all(jinja2.nodes.Call))

    config_func_calls = list(
        filter(
            lambda f: hasattr(f, "node") and hasattr(f.node, "name") and f.node.name == "config",  # type: ignore
            func_calls,
        )
    )
    # There should only be one {{ config(...) }} call per input
    config_func_call = config_func_calls[0] if config_func_calls else None

    if not config_func_call:
        return None

    unrendered_config = {}

    # Handle keyword arguments
    for kwarg in config_func_call.kwargs:
        unrendered_config[kwarg.key] = construct_static_kwarg_value(kwarg, string)

    # Handle dictionary literal arguments (e.g., config({'pre-hook': 'select 1'}))
    for arg in config_func_call.args:
        if isinstance(arg, jinja2.nodes.Dict):
            # Extract key-value pairs from the dictionary
            for pair in arg.items:
                if isinstance(pair.key, jinja2.nodes.Const):
                    key = pair.key.value
                    # Always extract from source to preserve original formatting
                    value_source = _extract_dict_value_from_source(string, key)
                    unrendered_config[key] = value_source

    return unrendered_config if unrendered_config else None


def _extract_dict_value_from_source(source_string: str, key: str) -> str:
    """Extract a dictionary value from source string.

    This is used for dictionary literal arguments like config({'key': value}).
    Handles both single and double quotes for keys.
    """
    import re

    # Find the config( and the dictionary
    config_match = re.search(r"\{\{\s*config\s*\(\s*\{", source_string)
    if not config_match:
        return str(key)  # Fallback

    # Try to find the key with both single and double quotes
    # First try with the format as it appears in the source (single quotes by default from repr)
    key_patterns = [
        rf"{re.escape(repr(key))}\s*:\s*",  # 'key': value
        rf'"{re.escape(key)}"\s*:\s*',  # "key": value
    ]

    key_match = None
    for pattern in key_patterns:
        key_pattern = re.compile(pattern, re.MULTILINE)
        key_match = key_pattern.search(source_string, config_match.end())
        if key_match:
            break

    if key_match:
        value_start = key_match.end()
        extractor = _SourceCodeExtractor(source_string)
        # Stop at comma or closing brace
        source_value = extractor.extract_until_delimiter(value_start, delimiters=(",", "}"))
        # Clean up: strip any trailing delimiters that shouldn't be included
        source_value = source_value.rstrip(",}")
        if source_value:
            return source_value

    return repr(key)  # Fallback


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

    def extract_until_delimiter(self, start_pos: int, delimiters: tuple = (",", ")")) -> str:
        """Extract source code from start_pos until a top-level delimiter is found.

        Args:
            start_pos: Position to start extraction from
            delimiters: Tuple of delimiter characters to stop at (when at nesting level 0)

        Returns:
            Extracted source code string, stripped of leading/trailing whitespace

        Example:
            For "func(a, b), x" with start_pos=5 and delimiters=(',',):
            Returns "a, b)"  (stops at the comma after the closing paren)
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

        return self.source[start_pos:end_pos].strip().rstrip(",")


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
            source_value = extractor.extract_until_delimiter(value_start, delimiters=(",", ")"))

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
    """
    Returns a RefArgs or List[str] object, corresponding to ref or source respectively, given an input jinja expression.

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
