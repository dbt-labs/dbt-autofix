"""Tests for inline-comment preservation when keys are moved between mappings.

ruamel.yaml stores a key's end-of-line comment on the *parent* mapping
(``ca.items[key]``), not on the value, so naively moving a value drops the
comment. ``copy_key_comment`` carries the inline note across while leaving
standalone trailing comments (which belong to whatever follows) in place.
"""

from ruamel.yaml.comments import CommentedMap

from dbt_autofix.refactors.changesets.dbt_schema_yml import restructure_yaml_keys_for_node
from dbt_autofix.refactors.yml import DbtYAML, copy_key_comment, load_yaml
from dbt_autofix.retrieve_schemas import SchemaSpecs


def _dump(data) -> str:
    return DbtYAML().dump_to_string(data)


# --- copy_key_comment unit behaviour ------------------------------------


def test_inline_comment_moves_with_key():
    src = load_yaml("a:\n  x: 1 # keep me\n")["a"]
    dst = CommentedMap()
    dst["x"] = src["x"]
    copy_key_comment(src, "x", dst)
    del src["x"]
    assert _dump(dst) == "x: 1 # keep me"


def test_inline_comment_moves_to_renamed_key():
    src = load_yaml("a:\n  old: 1 # keep me\n")["a"]
    src["new"] = src["old"]
    copy_key_comment(src, "old", src, "new")
    del src["old"]
    assert _dump(src) == "new: 1 # keep me"


def test_inline_comment_normalised_to_single_space():
    # Spacing is normalised to one space so the comment renders predictably
    # regardless of the destination's indent level.
    src = load_yaml("a:\n  x: 1  # two spaces\n")["a"]
    dst = CommentedMap()
    dst["x"] = src["x"]
    copy_key_comment(src, "x", dst)
    assert _dump(dst) == "x: 1 # two spaces"


def test_standalone_trailing_comment_does_not_move():
    # The comment documents the *next* key; moving 'x' must leave it behind.
    src = load_yaml("a:\n  x: 1\n\n  # belongs to y\n  y: 2\n")["a"]
    dst = CommentedMap()
    dst["x"] = src["x"]
    copy_key_comment(src, "x", dst)
    assert _dump(dst) == "x: 1"


def test_mixed_inline_and_trailing_only_moves_inline():
    src = load_yaml("a:\n  x: 1 # inline\n  # standalone\n  y: 2\n")["a"]
    dst = CommentedMap()
    dst["x"] = src["x"]
    copy_key_comment(src, "x", dst)
    assert _dump(dst) == "x: 1 # inline"


def test_no_comment_is_a_noop():
    src = load_yaml("a:\n  x: 1\n")["a"]
    dst = CommentedMap()
    dst["x"] = src["x"]
    copy_key_comment(src, "x", dst)
    assert "x" not in dst.ca.items


def test_plain_dict_destination_is_a_noop():
    src = load_yaml("a:\n  x: 1 # note\n")["a"]
    dst = {}  # plain dict cannot hold comments
    copy_key_comment(src, "x", dst)  # must not raise


# --- end-to-end through restructure_yaml_keys_for_node ----------------------


def _restructure_column(yaml_str: str) -> str:
    specs = SchemaSpecs()
    doc = load_yaml(yaml_str)
    for column in doc["models"][0]["columns"]:
        restructure_yaml_keys_for_node(column, "columns", specs)
    return _dump(doc)


def test_column_meta_move_preserves_comment():
    out = _restructure_column(
        "models:\n"
        "  - name: model1\n"
        "    columns:\n"
        "      - name: column1\n"
        "        meta:\n"
        "          foo_bar_baz: true # my awesome comment\n"
    )
    assert "foo_bar_baz: true # my awesome comment" in out
    assert "config:\n          meta:" in out


def test_column_meta_merge_preserves_both_comments():
    out = _restructure_column(
        "models:\n"
        "  - name: model1\n"
        "    columns:\n"
        "      - name: column1\n"
        "        meta:\n"
        "          a: 1 # note a\n"
        "        config:\n"
        "          meta:\n"
        "            b: 2 # note b\n"
    )
    assert "a: 1 # note a" in out
    assert "b: 2 # note b" in out


def test_unknown_field_move_preserves_comment():
    out = _restructure_column(
        "models:\n  - name: model1\n    columns:\n      - name: column1\n        some_unknown_field: 7 # trailing note\n"
    )
    assert "some_unknown_field: 7 # trailing note" in out
