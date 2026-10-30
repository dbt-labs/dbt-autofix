import io
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

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


def get_list(node: CommentedMap, key: str) -> CommentedSeq:
    return node.get(key) or CommentedSeq()


def get_dict(node: CommentedMap, key: str) -> CommentedMap:
    return node.get(key) or CommentedMap()


def iter_project_yaml_files(root_path: Path, dbt_paths: Iterable[str]) -> List[Path]:
    """Return every .yml/.yaml file under the given dbt paths, each once, in sorted order.

    Paths nested under another given path (e.g. `models` and `models/semantic`) are only walked once.
    """
    roots = sorted({(root_path / dbt_path).resolve() for dbt_path in dbt_paths}, key=lambda p: len(p.parts))
    covering_roots: List[Path] = []
    for root in roots:
        if root.is_dir() and not any(root.is_relative_to(parent) for parent in covering_roots):
            covering_roots.append(root)

    yaml_files = set()
    for root in covering_roots:
        yaml_files.update(root.glob("**/*.yml"))
        yaml_files.update(root.glob("**/*.yaml"))
    return sorted(yaml_files)


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
