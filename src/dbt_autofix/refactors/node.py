import copy
from dataclasses import dataclass, field
from typing import Any, Optional

from ruamel.yaml.comments import CommentedSeq

from dbt_autofix.refactors.results import Location, location_of_key, location_of_node


@dataclass
class Node:
    value: Any
    original_location: Optional[Location]
    comments: Optional[list] = field(default=None)


def extract_node(parent: Any, key: Any, original_parent: Any = None) -> Node:
    """Read parent[key], capture its original location and comments. Does NOT modify parent."""
    value = parent[key]
    if original_parent is None:
        original_parent = parent
    if isinstance(parent, CommentedSeq):
        original_location = location_of_node(value)
    else:
        original_location = location_of_key(original_parent, key)
    comments = copy.deepcopy(parent.ca.items.get(key)) if hasattr(parent, "ca") else None
    return Node(value=value, original_location=original_location, comments=comments)


def pop_node(parent: Any, key: Any, original_parent: Any = None) -> Node:
    """Read parent[key], capture its location and comments, then delete it from parent."""
    node = extract_node(parent, key, original_parent=original_parent)
    if hasattr(parent, "ca") and key in parent.ca.items:
        del parent.ca.items[key]
    del parent[key]
    return node


def assign_node(parent: Any, key: Any, node: Node, position: Optional[int] = None) -> None:
    """Set parent[key] = node.value and restore comment metadata."""
    if position is not None and hasattr(parent, "insert"):
        parent.insert(position, key, node.value)
    else:
        parent[key] = node.value
    if node.comments is not None and hasattr(parent, "ca"):
        parent.ca.items[key] = node.comments
