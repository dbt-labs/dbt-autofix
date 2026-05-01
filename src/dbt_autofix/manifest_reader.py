import json
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ModelInfo:
    unique_id: str
    name: str
    original_file_path: str   # e.g. "models/customers.sql"
    patch_path: Optional[str]  # e.g. "jaffle_shop://models/schema.yml" or None


def load_manifest(project_path: Path) -> dict:
    """Load and return the raw manifest dict from target/manifest.json."""
    manifest_path = project_path / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"manifest.json not found at {manifest_path}. Run `dbtf compile` first."
        )
    with open(manifest_path) as f:
        return json.load(f)


def get_model_infos(manifest: dict) -> dict[str, "ModelInfo"]:
    """Return a dict of unique_id → ModelInfo for all model nodes."""
    return {
        uid: ModelInfo(
            unique_id=uid,
            name=node["name"],
            original_file_path=node["original_file_path"],
            patch_path=node.get("patch_path"),
        )
        for uid, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    }


def match_compiled_paths_to_unique_ids(
    manifest: dict,
    compiled_paths: list[Path],
    project_path: Path,
) -> dict[Path, str]:
    """Map each compiled SQL path to its model unique_id via original_file_path."""
    # Build lookup: original_file_path (forward-slash, relative) → unique_id
    lookup: dict[str, str] = {
        node["original_file_path"].replace("\\", "/"): uid
        for uid, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    }

    compiled_root = project_path / "target" / "compiled"
    result: dict[Path, str] = {}

    for compiled_path in compiled_paths:
        try:
            # Strip project_path/target/compiled/<project_name>/ to get original_file_path
            rel = compiled_path.relative_to(compiled_root)
            # rel = <project_name>/models/foo.sql — drop the first component (project name)
            parts = rel.parts
            if len(parts) < 2:
                continue
            original = "/".join(parts[1:])  # drop project_name prefix
            if original in lookup:
                result[compiled_path] = lookup[original]
        except ValueError:
            continue

    return result


def get_transitive_descendants(manifest: dict, unique_ids: set[str]) -> set[str]:
    """Return all transitive downstream model unique_ids for the given seed set.

    Only includes nodes with resource_type == "model". The input unique_ids are
    NOT included in the result.
    """
    model_ids: set[str] = {
        uid
        for uid, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    }
    child_map: dict[str, list[str]] = manifest.get("child_map", {})

    visited: set[str] = set()
    queue: deque[str] = deque(unique_ids)

    while queue:
        current = queue.popleft()
        for child in child_map.get(current, []):
            if child in model_ids and child not in visited and child not in unique_ids:
                visited.add(child)
                queue.append(child)

    return visited
