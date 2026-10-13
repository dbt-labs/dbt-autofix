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


def copy_key_comment(
    src: CommentedMap,
    src_key: Any,
    dst: CommentedMap,
    dst_key: Any = None,
) -> None:
    """Copies a keys inline comment when moving it between mappings.

    ruamel.yaml stores a key's end-of-line comment on the parent mapping
    (``src.ca.items[src_key]``), not on the value object, so moving only the value
    (``dst[key] = src[key]``) drops it.

    Call copy_key_comment BEFORE deleting ``src[src_key]`` to preserve the comment.

    Only the actual inline comment (``key: value # note`` on the same line) is copied.

    A trailing/standalone comment block (a blank line then ``# ...`` that documents whatever follows) is left in place, as
    ruamel records it against the preceding key too, but it logically belongs to the source location, so relocating it
    with the moved key would be wrong.

    no-op when there is no inline comment or when ``dst`` cannot hold one.
    """
    if dst_key is None:
        dst_key = src_key
    src_ca = getattr(src, "ca", None)
    if src_ca is None or src_key not in src_ca.items:
        return
    if not hasattr(dst, "yaml_add_eol_comment"):
        return
    token = src_ca.items[src_key][2]
    if token is None:
        return
    # The inline comment sits on the same line as the value, so the token text
    # starts with '#'. A standalone trailing block starts with a newline instead
    # and logically belongs to whatever follows, so it must not move.
    inline = token.value.split("\n", 1)[0].lstrip()
    if not inline.startswith("#"):
        return
    # column=0 renders the comment one space after the value, regardless of the
    # destination's indent (a preserved absolute column would pad oddly when the
    # key moves to a shallower level).
    dst.yaml_add_eol_comment(inline, key=dst_key, column=0)


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
