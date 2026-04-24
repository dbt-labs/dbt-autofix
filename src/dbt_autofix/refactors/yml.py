import io
import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Union

import yamllint.config
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.compat import StringIO

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)

logger = logging.getLogger(__name__)

# dbt YAML is UTF-8; use everywhere we read project YAML from disk
ENCODING_YAML = "utf-8"
_YAML_READ_KW: dict[str, Any] = {"encoding": ENCODING_YAML, "errors": "replace"}


def read_project_yaml_text(path: Path) -> str:
    """Read project YAML from disk (UTF-8, replacement on undecodable bytes)."""
    return path.read_text(**_YAML_READ_KW)


def _path_is_descendant_of(child: Path, parent: Path) -> bool:
    c, p = child.resolve(), parent.resolve()
    if c == p:
        return True
    try:
        c.relative_to(p)
        return True
    except ValueError:
        return False


def _minimal_covering_path_roots(bases: Iterable[Path]) -> list[Path]:
    """Drop dbt model path roots that are under another root so we do not glob the same tree twice."""
    unique: Set[Path] = {b.resolve() for b in bases}
    if not unique:
        return []
    if len(unique) == 1:
        return [next(iter(unique))]
    # Parents before children
    ordered = sorted(unique, key=lambda p: (len(p.parts), str(p)))
    out: list[Path] = []
    for p in ordered:
        if any(_path_is_descendant_of(p, q) for q in out):
            continue
        out.append(p)
    return out


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


@lru_cache(maxsize=1)
def get_dbt_yaml() -> DbtYAML:
    """Return a process-wide `DbtYAML` used for load/dump to avoid allocator churn (CLI is single-threaded)."""
    return DbtYAML()


def get_list(node: CommentedMap, key: str) -> CommentedSeq:
    return node.get(key) or CommentedSeq()


def get_dict(node: CommentedMap, key: str) -> CommentedMap:
    return node.get(key) or CommentedMap()


def iter_project_yaml_files(root_path: Path, model_paths: Iterable[str]) -> list[Path]:
    """All distinct ``.yml`` / ``.yaml`` paths under the given dbt model path roots, sorted for stable runs.

    ``model_paths`` is deduplicated (first occurrence wins) so repeated dbt path entries do not re-glob.
    Overlapping roots (e.g. ``models`` and ``models/semantic``) are reduced to the minimal covering set
    so nested trees are not walked twice.
    """
    unique_strs = list(dict.fromkeys(str(p) for p in model_paths))
    resolved_bases = [(root_path / Path(mp)).resolve() for mp in unique_strs]
    existing = [b for b in resolved_bases if b.exists()]
    to_scan = _minimal_covering_path_roots(existing) if len(existing) > 1 else existing
    paths: set[Path] = set()
    for base in to_scan:
        paths.update(base.glob("**/*.yml"))
        paths.update(base.glob("**/*.yaml"))
    return sorted(paths)


def _yaml_cache_include_file_text() -> bool:
    """If true, ``ProjectYamlCache.text_by_path`` is filled so ``process_yaml`` can skip a second disk read (memory)."""
    v = (os.environ.get("DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


@dataclass
class ProjectYamlCache:
    """Glob once + one parse per YAML on disk.

    Invariants: ``ordered_paths`` contains exactly the paths used as keys in ``parsed_by_path`` and (when set)
    ``text_by_path``. Callers that walk ``ordered_paths`` and look up parsed/text must use the same ``Path``
    object instances as keys (not a path re-resolved from a string) so dict lookups match.

    By default original text is not kept (re-read in ``process_yaml`` for memory). Set env
    ``DBT_AUTOFIX_YAML_CACHE_INCLUDE_TEXT=1`` to keep file text in ``text_by_path`` and avoid that second read.
    """

    ordered_paths: list[Path]
    parsed_by_path: dict[Path, CommentedMap] = field(repr=False)
    text_by_path: Optional[dict[Path, str]] = field(default=None, repr=False)


def build_project_yaml_cache(root_path: Path, model_paths: Iterable[str]) -> ProjectYamlCache:
    """List all project YAML files once, then read and parse each file once (``load_yaml`` per file)."""
    ordered_paths = iter_project_yaml_files(root_path, model_paths)
    parsed_by_path: dict[Path, CommentedMap] = {}
    text_by_path: dict[Path, str] | None = {} if _yaml_cache_include_file_text() else None
    for p in ordered_paths:
        t = read_project_yaml_text(p)
        if text_by_path is not None:
            text_by_path[p] = t
        try:
            parsed_by_path[p] = load_yaml(t)
        except Exception as e:
            logger.warning("YAML parse failed for %s, using empty map: %s: %s", p, type(e).__name__, e)
            parsed_by_path[p] = CommentedMap()
    return ProjectYamlCache(ordered_paths, parsed_by_path, text_by_path)


def load_yaml(path_or_str: Union[Path, str]) -> CommentedMap:
    """Load YAML. Paths are read with ``read_project_yaml_text`` so encoding matches the rest of the project."""
    text = read_project_yaml_text(path_or_str) if isinstance(path_or_str, Path) else path_or_str
    return get_dbt_yaml().load(text) or CommentedMap()


def dict_to_yaml_str(content: Dict[str, Any], write_empty: bool = False) -> str:
    """Write a dict value to a YAML string"""

    # If content is empty, return an empty string
    if not content and write_empty:
        return ""

    return get_dbt_yaml().dump_to_string(content)
