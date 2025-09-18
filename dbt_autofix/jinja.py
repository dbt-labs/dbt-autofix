from dataclasses import dataclass
from typing import Any, Dict, Optional

import jinja2

from dbt_common.clients.jinja import get_environment
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore


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
    env = get_environment(None, capture_macros=True)

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
        return None

    unrendered_config = {}
    for kwarg in config_func_call.kwargs:
        unrendered_config[kwarg.key] = construct_static_kwarg_value(kwarg)

    return unrendered_config


def construct_static_kwarg_value(kwarg) -> str:
    # Instead of trying to re-assemble complex kwarg value, simply stringify the value.
    # This is still useful to be able to detect changes in unrendered configs, even if it is
    # not an exact representation of the user input.
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
