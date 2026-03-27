import copy
from dataclasses import dataclass, field
from typing import Any, Optional

from ruamel.yaml.comments import CommentedMap, CommentedSeq

from dbt_autofix.refactors.results import Location, location_of_key, location_of_node
from dbt_autofix.refactors.yml import CA_AFTER_IDX, extract_above_comment, set_above_comment


@dataclass
class Node:
    value: Any
    original_location: Optional[Location]
    comments: Optional[list] = field(default=None)
    above_comment: Any = field(default=None)


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
    node.above_comment = extract_above_comment(parent, key)
    if hasattr(parent, "ca") and key in parent.ca.items:
        del parent.ca.items[key]
    del parent[key]
    return node


def extract_deep_trailing_above_comment(value: Any) -> Any:
    """Walk the deepest CommentedMap/CommentedSeq chain and extract the above-comment token.

    ruamel.yaml stores the above-comment for the next top-level key as the trailing part
    of the deepest last-key token in the preceding sibling's value subtree. This function
    walks that chain (last CommentedSeq item → last CommentedMap key, recursively), finds
    the deepest CA_AFTER_IDX token, and splits it at the first newline: the inline part is
    kept in place, and the above-comment part (stripped of leading newlines) is returned.

    Returns the above-comment token, or None if not found.
    """
    if isinstance(value, CommentedSeq) and value:
        return extract_deep_trailing_above_comment(value[-1])
    if isinstance(value, CommentedMap) and value:
        last_key = list(value.keys())[-1]
        deep = extract_deep_trailing_above_comment(value[last_key])
        if deep is not None:
            return deep
        if not hasattr(value, "ca") or last_key not in value.ca.items:
            return None
        ca = value.ca.items[last_key]
        if not ca or len(ca) <= CA_AFTER_IDX or ca[CA_AFTER_IDX] is None:
            return None
        token = ca[CA_AFTER_IDX]
        if not hasattr(token, "value"):
            return None
        first_nl = token.value.find("\n")
        if first_nl == -1:
            return None
        above_part = token.value[first_nl + 1 :]
        if not above_part.strip():
            return None
        inline_tok = copy.deepcopy(token)
        inline_tok.value = token.value[: first_nl + 1]
        ca[CA_AFTER_IDX] = inline_tok
        above_tok = copy.deepcopy(token)
        above_tok.value = above_part.lstrip("\n")
        return above_tok
    return None


def insert_at_deep_trailing(value: Any, comment_tok: Any) -> bool:
    r"""Insert comment_tok at the start of the above-comment part of the deepest trailing token.

    Walks the deepest CommentedMap/CommentedSeq chain, finds the CA_AFTER_IDX token, and
    inserts comment_tok.value after the inline part (first \n) but before any existing
    above-comments.

    Returns True if inserted successfully.
    """
    if isinstance(value, CommentedSeq) and value:
        return insert_at_deep_trailing(value[-1], comment_tok)
    if not isinstance(value, CommentedMap) or not value:
        return False
    last_key = list(value.keys())[-1]
    last_val = value[last_key]
    if isinstance(last_val, (CommentedMap, CommentedSeq)) and last_val:
        if insert_at_deep_trailing(last_val, comment_tok):
            return True
    if not hasattr(value, "ca"):
        return False
    existing_ca = value.ca.items.get(last_key)
    if existing_ca is None:
        existing_ca = [None, None, None, None]
        value.ca.items[last_key] = existing_ca

    existing = existing_ca[CA_AFTER_IDX] if len(existing_ca) > CA_AFTER_IDX else None
    comment_value = comment_tok.value
    if not comment_value.endswith("\n"):
        comment_value += "\n"

    if existing is None or not hasattr(existing, "value"):
        new_tok = copy.deepcopy(comment_tok)
        new_tok.value = "\n" + comment_value
        existing_ca[CA_AFTER_IDX] = new_tok
    else:
        first_nl = existing.value.find("\n")
        if first_nl == -1:
            new_tok = copy.deepcopy(existing)
            new_tok.value = existing.value + comment_value
            existing_ca[CA_AFTER_IDX] = new_tok
        else:
            inline = existing.value[: first_nl + 1]
            above = existing.value[first_nl + 1 :]
            new_tok = copy.deepcopy(existing)
            new_tok.value = inline + comment_value + above
            existing_ca[CA_AFTER_IDX] = new_tok

    return True


def delete_top_level_key(yml_dict: Any, key: Any) -> None:
    """Delete a key from yml_dict, properly handling comment metadata.

    Works for both composite (CommentedMap/CommentedSeq) and scalar predecessor values.
    Steps:

    1. Discard the above-comment for `key` from `prev_key`'s trailing.
    2. Extract the above-comment for `next_key` from `key`'s deep trailing.
    3. Delete `key` and its orphaned ca.items entry.
    4. Re-attach the above-comment for `next_key` after `prev_key`.
    """
    if key not in yml_dict:
        return
    keys = list(yml_dict.keys())
    idx = keys.index(key)
    prev_key = keys[idx - 1] if idx > 0 else None
    next_key = keys[idx + 1] if idx + 1 < len(keys) else None

    # 1. Discard the above-comment for `key`.
    if prev_key is not None:
        prev_val = yml_dict[prev_key]
        if isinstance(prev_val, (CommentedMap, CommentedSeq)):
            extract_deep_trailing_above_comment(prev_val)
        else:
            extract_above_comment(yml_dict, key)

    # 2. Extract the above-comment for `next_key` from `key`'s deep trailing.
    next_above = extract_deep_trailing_above_comment(yml_dict[key]) if next_key is not None else None

    # 3. Delete `key` and its orphaned ca.items entry.
    if hasattr(yml_dict, "ca") and key in yml_dict.ca.items:
        del yml_dict.ca.items[key]
    del yml_dict[key]

    # 4. Re-attach the above-comment for `next_key` after `prev_key`.
    if prev_key is not None and next_above is not None:
        prev_val = yml_dict[prev_key]
        if isinstance(prev_val, (CommentedMap, CommentedSeq)):
            insert_at_deep_trailing(prev_val, next_above)
        else:
            set_above_comment(yml_dict, next_key, next_above)


def assign_node(parent: Any, key: Any, node: Node, position: Optional[int] = None) -> None:
    """Set parent[key] = node.value and restore comment metadata."""
    if position is not None and hasattr(parent, "insert"):
        parent.insert(position, key, node.value)
    else:
        parent[key] = node.value
    if node.comments is not None and hasattr(parent, "ca"):
        parent.ca.items[key] = node.comments
    set_above_comment(parent, key, node.above_comment)


def extract_nodes_by_name(seq: CommentedSeq) -> dict:
    """Return a {name: Node} mapping for every item in a named sequence.

    Captures each item's original location and seq-item comments via extract_node,
    keyed by the item's ``name`` field. Items without a ``name`` are skipped.
    """
    return {item.get("name"): extract_node(seq, i) for i, item in enumerate(seq) if item.get("name") is not None}


def reattach_next_key_above_comment(node: Node, parent: CommentedMap, next_key: Any) -> None:
    r"""Split node.comments[CA_AFTER_IDX] after popping its key from parent.

    The CA_AFTER_IDX slot may hold both an inline comment for the popped key and
    the above-comment for next_key (content after the first ``\n``). This function
    keeps the inline part in node.comments and re-attaches the above-comment portion
    to next_key in parent via set_above_comment.
    """
    if (
        next_key is None
        or node.comments is None
        or len(node.comments) <= CA_AFTER_IDX
        or node.comments[CA_AFTER_IDX] is None
        or not hasattr(node.comments[CA_AFTER_IDX], "value")
    ):
        return
    after_tok = node.comments[CA_AFTER_IDX]
    first_nl = after_tok.value.find("\n")
    if first_nl == -1:
        return
    above_part = after_tok.value[first_nl + 1 :]
    if not above_part.strip():
        return
    inline_tok = copy.deepcopy(after_tok)
    inline_tok.value = after_tok.value[: first_nl + 1]
    node.comments[CA_AFTER_IDX] = inline_tok
    above_tok = copy.deepcopy(after_tok)
    above_tok.value = above_part
    set_above_comment(parent, next_key, above_tok)


def append_node(seq: CommentedSeq, node: Node) -> None:
    """Append node.value to seq and restore its seq-item comment metadata.

    The seq-item comment (the [before, inline, after, eol] slot that lives on
    the parent sequence, not inside the item's own CommentedMap) must be
    explicitly re-attached after appending because .append() creates a fresh
    slot with no comment data.

    Use extract_node(source_seq, i) to capture the Node before moving an item.
    """
    idx = len(seq)
    seq.append(node.value)
    if node.comments is not None and hasattr(seq, "ca"):
        seq.ca.items[idx] = node.comments
