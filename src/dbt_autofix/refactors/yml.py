import copy
import io
from pathlib import Path
from typing import Any, Dict, Union

import yamllint.config
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.compat import StringIO

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)

# ruamel.yaml ca.items slot indices: ca.items[key] = [before, inline, after, eol]
CA_BEFORE_IDX = 0
CA_INLINE_IDX = 1
CA_AFTER_IDX = 2
CA_EOL_IDX = 3


class DbtYAML(YAML):
    """dbt-compatible YAML class."""

    def __init__(self):
        super().__init__(typ=["rt", "string"])
        self.preserve_quotes = True
        self.width = 4096
        self.indent(mapping=2, sequence=4, offset=2)
        self.default_flow_style = False

    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        super().dump(data, stream, **kw)
        if inefficient:
            return stream.getvalue()

    def dump_to_string(self, data: Any, add_final_eol: bool = False) -> str:
        buf = io.BytesIO()
        self.dump(data, buf)
        if add_final_eol:
            return buf.getvalue().decode("utf-8")
        else:
            return buf.getvalue()[:-1].decode("utf-8")


def copy_ca(source: CommentedMap, dest: CommentedMap) -> None:
    """Copy the .ca.items dict from source to dest (deep copy).

    Call this after CommentedMap.copy() to preserve comment metadata
    that the shallow copy drops.
    """
    dest.ca.items.update(copy.deepcopy(source.ca.items))


def set_blank_line_before(node: CommentedMap, key: Any) -> None:
    """Inject a blank-line CommentToken before key in node.

    In ruamel.yaml, blank lines between map keys are stored as the inline
    comment (position [2]) of the **preceding** key, not on the key itself.
    This function finds the key before `key` and sets its inline comment to
    include a leading newline, creating a blank line in the serialized output.

    Call this after inserting a new key that should be separated from the
    preceding section by a blank line.
    """
    from ruamel.yaml.error import CommentMark
    from ruamel.yaml.tokens import CommentToken

    keys = list(node.keys())
    idx = keys.index(key) if key in keys else -1
    if idx <= 0:
        return
    prev_key = keys[idx - 1]
    existing = node.ca.items.get(prev_key, [None, None, None, None])
    if existing[CA_AFTER_IDX] is None:
        existing[CA_AFTER_IDX] = CommentToken("\n\n", CommentMark(0), None)
    node.ca.items[prev_key] = existing


def extract_preceding_text_comment(yml_dict: CommentedMap, key: Any) -> Any:
    """Extract a text comment stored as the trailing token of the key preceding `key`.

    ruamel.yaml sometimes stores a comment that visually appears before `key` as the
    trailing (CA_AFTER_IDX) token of the preceding key. When we move `key` elsewhere,
    we need to move that comment too.

    Returns the comment token and clears it from the preceding key, or None if absent.
    """
    if not hasattr(yml_dict, "ca"):
        return None
    keys = list(yml_dict.keys())
    idx = keys.index(key)
    if idx == 0:
        return None
    prev_key = keys[idx - 1]
    if prev_key not in yml_dict.ca.items:
        return None
    ca = yml_dict.ca.items[prev_key]
    if ca is None or len(ca) <= CA_AFTER_IDX or ca[CA_AFTER_IDX] is None:
        return None
    token = ca[CA_AFTER_IDX]
    if hasattr(token, "value") and token.value.strip().startswith("#"):
        ca[CA_AFTER_IDX] = None
        # Strip leading newlines: the token was a trailing comment of the previous key,
        # so its value begins with '\n' (the line-ending of that key). That '\n' would
        # produce a spurious blank line when the token is placed as a before-key comment.
        token.value = token.value.lstrip()
        return token
    return None


def extract_trailing_separator(yml_dict: Any) -> Any:
    """Remove and return the CA_AFTER_IDX trailing-separator token from the deepest last-key chain.

    ruamel attaches blank lines and comments that follow a block to the CA_AFTER_IDX slot of the
    deepest last key reachable by always taking the last key at each level. This mirrors
    the parser's behaviour: by the time it sees a trailing comment it has already finished
    the last key, so it stores the token there rather than doing lookahead to find the next
    key. As a result a separator visually between two top-level siblings can be buried
    several levels deep inside the preceding sibling's subtree.

    This function walks that last-key chain recursively and extracts the token, clearing
    the slot so it can be relocated. Returns None if no token is found.
    """
    if not isinstance(yml_dict, CommentedMap) or not yml_dict:
        return None
    last_key = list(yml_dict.keys())[-1]
    last_value = yml_dict.get(last_key)
    if isinstance(last_value, CommentedMap) and last_value:
        sep = extract_trailing_separator(last_value)
        if sep is not None:
            return sep
    if hasattr(yml_dict, "ca") and last_key in yml_dict.ca.items:
        ca = yml_dict.ca.items[last_key]
        if ca is not None and len(ca) > CA_AFTER_IDX and ca[CA_AFTER_IDX] is not None:
            sep = ca[CA_AFTER_IDX]
            ca[CA_AFTER_IDX] = None
            return sep
    return None


def set_trailing_separator(yml_dict: Any, sep: Any) -> None:
    """Write a trailing-separator token to the deepest last-key chain of yml_dict.

    Mirrors extract_trailing_separator: walks to the deepest last key and places the
    token in its CA_AFTER_IDX slot, which is where ruamel will emit it — after the
    block's last value and before whatever comes next at the outer level.
    """
    if not isinstance(yml_dict, CommentedMap) or not yml_dict:
        return
    last_key = list(yml_dict.keys())[-1]
    last_value = yml_dict.get(last_key)
    if isinstance(last_value, CommentedMap) and last_value:
        set_trailing_separator(last_value, sep)
        return
    if not hasattr(yml_dict, "ca"):
        return
    if last_key not in yml_dict.ca.items or yml_dict.ca.items[last_key] is None:
        yml_dict.ca.items[last_key] = [None, None, sep, None]
    else:
        yml_dict.ca.items[last_key][CA_AFTER_IDX] = sep


def rebalance_trailing_separator(yml_dict: CommentedMap, k: Any, original_keys: set) -> None:
    """Fix trailing-separator displacement caused by inserting a new key after child k.

    ruamel stores blank lines and comments that appear *after* a block as the CA_AFTER_IDX
    slot of the deepest last-key in the block's subtree. When we refactor the content of k
    (e.g. moving a key to +meta), that deep last-key changes, and the separator token can
    travel to the wrong structural position. Additionally, if a new key (such as `+meta`)
    was appended to yml_dict *after* k during an earlier iteration, k is no longer
    yml_dict's last key, so the separator should logically belong to yml_dict's actual
    last key, not inside k's subtree.

    This function detects that situation (current last key of yml_dict is not k and was
    not present in the original snapshot) and re-attaches the separator to the correct
    position via extract_trailing_separator / set_trailing_separator.
    """
    current_last_key = list(yml_dict.keys())[-1]
    if current_last_key != k and current_last_key not in original_keys:
        sep = extract_trailing_separator(yml_dict[k])
        if sep is not None:
            set_trailing_separator(yml_dict, sep)


def get_list(node: CommentedMap, key: str) -> CommentedSeq:
    return node.get(key) or CommentedSeq()


def get_dict(node: CommentedMap, key: str) -> CommentedMap:
    return node.get(key) or CommentedMap()


def load_yaml(path_or_str: Union[Path, str]) -> CommentedMap:
    yaml = DbtYAML()
    return yaml.load(path_or_str) or CommentedMap()


def dict_to_yaml_str(content: Dict[str, Any], write_empty: bool = False) -> str:
    """Write a dict value to a YAML string"""

    # If content is empty, return an empty string
    if not content and write_empty:
        return ""

    yaml = DbtYAML()
    file_text = yaml.dump_to_string(content)
    return file_text
