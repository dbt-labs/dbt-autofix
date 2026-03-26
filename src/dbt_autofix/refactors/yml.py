import copy
import io
from pathlib import Path
from typing import Any, Dict, Union

import yamllint.config
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.compat import StringIO
from ruamel.yaml.error import CommentMark
from ruamel.yaml.tokens import CommentToken

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
        first_newline = token.value.find("\n")
        if first_newline != -1 and not token.value.startswith("\n"):
            # Token contains both the inline comment for prev_key AND the above-comment
            # for the key being moved (e.g. "# prev - inline\n# key\n"). Split them:
            # keep the inline part with prev_key, return only the above-comment.
            inline_part = token.value[: first_newline + 1]
            above_part = token.value[first_newline + 1 :]
            ca[CA_AFTER_IDX] = copy.deepcopy(token)
            ca[CA_AFTER_IDX].value = inline_part
            if not above_part.strip():
                return None
            above_token = copy.deepcopy(token)
            above_token.value = above_part
            return above_token
        ca[CA_AFTER_IDX] = None
        # Strip leading newlines: the token was a trailing comment of the previous key,
        # so its value begins with '\n' (the line-ending of that key). That '\n' would
        # produce a spurious blank line when the token is placed as a before-key comment.
        token.value = token.value.lstrip()
        return token
    return None


def extract_above_comment(yml_dict: CommentedMap, key: Any) -> Any:
    """Extract and clear the above-comment for `key` in `yml_dict`.

    Handles both storage locations ruamel.yaml uses:
    - Index 0 (first key): the comment is embedded as the second line of ca.comment[0]
      (after the parent key's inline comment) or in ca.comment[1].
    - Index > 0 (non-first key): delegated to extract_preceding_text_comment.

    Returns the comment token (or list) and clears it from its original location, or
    returns None if no above-comment is found.
    """
    if not hasattr(yml_dict, "ca"):
        return None
    keys = list(yml_dict.keys())
    if key not in keys:
        return None
    idx = keys.index(key)
    if idx > 0:
        return extract_preceding_text_comment(yml_dict, key)
    # First key: above-comment is in ca.comment[1] or as the second line of ca.comment[0].
    if not yml_dict.ca.comment:
        return None
    if yml_dict.ca.comment[1] is not None:
        above_token = yml_dict.ca.comment[1]
        yml_dict.ca.comment[1] = None
        return above_token
    tok = yml_dict.ca.comment[0]
    if tok is None or not hasattr(tok, "value"):
        return None
    first_nl = tok.value.find("\n")
    if first_nl == -1:
        return None
    above_part = tok.value[first_nl + 1 :]
    if not above_part.strip():
        return None
    tok.value = tok.value[: first_nl + 1]
    above_token = copy.deepcopy(tok)
    above_token.value = above_part
    return above_token


def set_first_key_above_comment(mapping: Any, above_token: Any) -> None:
    """Attach `above_token` as the before-first-key comment of `mapping`.

    `above_token` may be a single CommentToken or a list of CommentTokens; both forms
    are accepted.

    When `mapping.ca.comment[0]` is a CommentToken (the combined inline+above-comment
    token that ruamel uses when the parent key has an inline comment), the above-comment
    is appended to that token's value in-place. This is necessary because
    `mapping.ca.comment[0]` is the same Python object as the parent mapping's
    `ca.items[key][CA_AFTER_IDX]`, so only in-place mutation propagates to the parent.

    Otherwise, the above-comment is stored in `ca.comment[1]` (the standard location).
    """
    if above_token is None or not hasattr(mapping, "ca"):
        return
    above_value = (
        "".join(t.value for t in above_token if hasattr(t, "value"))
        if isinstance(above_token, list)
        else (above_token.value if hasattr(above_token, "value") else "")
    )
    if not above_value:
        return
    if mapping.ca.comment and mapping.ca.comment[0] is not None and hasattr(mapping.ca.comment[0], "value"):
        # Append to the shared combined token in-place so the parent's ca.items also picks it up.
        mapping.ca.comment[0].value = mapping.ca.comment[0].value + above_value
        return
    token_list = above_token if isinstance(above_token, list) else [above_token]
    if mapping.ca.comment is None:
        mapping.ca.comment = [None, None]
    mapping.ca.comment[1] = token_list  # type: ignore[index]


def set_above_comment(mapping: Any, key: Any, above_token: Any) -> None:
    r"""Place `above_token` as the above-comment for `key` in `mapping`.

    Handles both positions:
    - First key (idx==0): stored in ca.comment[1] via set_first_key_above_comment.
    - Non-first key: stored in the preceding key's CA_AFTER_IDX slot.

    For the non-first-key case, CA_AFTER_IDX renders content before the first \n
    inline (on the same line as the key's value). The above-comment must therefore
    follow any existing inline content, separated by a \n.
    """
    if above_token is None or not hasattr(mapping, "ca"):
        return
    keys = list(mapping.keys())
    if key not in keys:
        return
    idx = keys.index(key)
    if idx == 0:
        set_first_key_above_comment(mapping, above_token)
    else:
        preceding_key = keys[idx - 1]
        existing = mapping.ca.items.get(preceding_key) or [None, None, None, None]
        existing_after = existing[CA_AFTER_IDX]
        if existing_after is not None and hasattr(existing_after, "value"):
            # Append above-comment after the existing CA_AFTER_IDX content.
            # The existing value ends with \n; above_token.value is the comment text.
            combined = copy.deepcopy(existing_after)
            combined.value = existing_after.value + above_token.value
            existing[CA_AFTER_IDX] = combined
        else:
            # No existing CA_AFTER_IDX: above_token must start with \n so it
            # renders on a new line rather than inline on the preceding key's line.
            token = copy.deepcopy(above_token)
            if not token.value.startswith("\n"):
                token.value = "\n" + token.value
            existing[CA_AFTER_IDX] = token
        mapping.ca.items[preceding_key] = existing


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
            # If the token starts with '#' (not '\n'), it begins with an inline comment for
            # the current key. Split at the first newline: keep the inline part in place and
            # return only the actual separator (blank lines + above-comment for the next key).
            if hasattr(sep, "value") and sep.value and not sep.value.startswith("\n"):
                first_nl = sep.value.find("\n")
                if first_nl == -1:
                    # Single-line inline comment only — no separator to return.
                    return None
                sep_part = sep.value[first_nl + 1 :]
                if not sep_part.strip():
                    # No meaningful separator after the inline comment.
                    return None
                inline_tok = copy.deepcopy(sep)
                inline_tok.value = sep.value[: first_nl + 1]
                ca[CA_AFTER_IDX] = inline_tok
                sep_tok = copy.deepcopy(sep)
                sep_tok.value = sep_part
                return sep_tok
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
        existing = yml_dict.ca.items[last_key][CA_AFTER_IDX]
        if existing is not None and hasattr(existing, "value") and sep is not None and hasattr(sep, "value"):
            # Append the separator after the existing content (e.g. inline comment) rather
            # than overwriting it, so that the inline comment is not lost.
            combined = copy.deepcopy(existing)
            combined.value = existing.value + sep.value
            yml_dict.ca.items[last_key][CA_AFTER_IDX] = combined
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
