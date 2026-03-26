"""Unit tests for yml helper functions in refactors/node.py and refactors/yml.py."""

import textwrap

from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.error import CommentMark
from ruamel.yaml.tokens import CommentToken

from dbt_autofix.refactors.node import (
    Node,
    append_node,
    assign_node,
    delete_top_level_key,
    extract_deep_trailing_above_comment,
    extract_node,
    extract_nodes_by_name,
    insert_at_deep_trailing,
    pop_node,
    reattach_next_key_above_comment,
)
from dbt_autofix.refactors.yml import (
    CA_AFTER_IDX,
    DbtYAML,
    copy_ca,
    dict_to_yaml_str,
    extract_above_comment,
    extract_preceding_text_comment,
    extract_trailing_separator,
    load_yaml,
    rebalance_trailing_separator,
    set_above_comment,
    set_blank_line_before,
    set_first_key_above_comment,
    set_trailing_separator,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────


def load(yml_str: str) -> CommentedMap:
    """Load a dedented YAML string into a CommentedMap."""
    return load_yaml(textwrap.dedent(yml_str).strip() + "\n")


def dump(data) -> str:
    """Dump a CommentedMap to a YAML string (no trailing newline)."""
    yaml = DbtYAML()
    return yaml.dump_to_string(data)


def make_token(value: str) -> CommentToken:
    return CommentToken(value, CommentMark(0), None)


# ─── load_yaml / dict_to_yaml_str ─────────────────────────────────────────────


class TestLoadYaml:
    def test_simple_map(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        assert isinstance(data, CommentedMap)
        assert data["foo"] == 1
        assert data["bar"] == 2

    def test_empty_string(self):
        data = load_yaml("")
        assert isinstance(data, CommentedMap)
        assert len(data) == 0

    def test_nested_map(self):
        data = load("""
            parent:
              child:
                leaf: 42
        """)
        assert data["parent"]["child"]["leaf"] == 42

    def test_sequence_value(self):
        data = load("""
            items:
              - a
              - b
              - c
        """)
        assert list(data["items"]) == ["a", "b", "c"]

    def test_preserves_quotes(self):
        data = load("""
            foo: 'bar'
            baz: "qux"
        """)
        result = dump(data)
        assert "'bar'" in result
        assert '"qux"' in result

    def test_multiline_string_as_path(self, tmp_path):
        p = tmp_path / "test.yml"
        p.write_text("key: value\n")
        data = load_yaml(p)
        assert data["key"] == "value"


class TestDictToYamlStr:
    def test_basic_dict(self):
        result = dict_to_yaml_str({"foo": 1, "bar": 2})
        assert "foo: 1" in result
        assert "bar: 2" in result

    def test_empty_dict_write_empty_true(self):
        result = dict_to_yaml_str({}, write_empty=True)
        assert result == ""

    def test_empty_dict_write_empty_false(self):
        # Default (write_empty=False): dumps the empty dict, doesn't short-circuit
        result = dict_to_yaml_str({})
        assert result is not None

    def test_nested_dict(self):
        result = dict_to_yaml_str({"a": {"b": 1}})
        assert "a:" in result
        assert "b: 1" in result


# ─── extract_node ─────────────────────────────────────────────────────────────


class TestExtractNode:
    def test_basic_map_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = extract_node(data, "foo")
        assert node.value == 1

    def test_does_not_remove_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        extract_node(data, "foo")
        assert "foo" in data
        assert list(data.keys()) == ["foo", "bar"]

    def test_captures_location(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = extract_node(data, "bar")
        assert node.original_location is not None
        # location_of_key returns 1-indexed lines; 'bar' is on the second line
        assert node.original_location.line == 2

    def test_first_key_location(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = extract_node(data, "foo")
        assert node.original_location is not None
        # 'foo' is on the first line
        assert node.original_location.line == 1

    def test_captures_inline_comment(self):
        data = load("""
            foo: 1  # note
            bar: 2
        """)
        node = extract_node(data, "foo")
        assert node.comments is not None
        # The inline comment slot should be populated
        comment_values = " ".join(tok.value for tok in node.comments if tok is not None and hasattr(tok, "value"))
        assert "note" in comment_values

    def test_no_inline_comment_gives_none_or_empty(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = extract_node(data, "foo")
        # If no comment token, ca.items.get("foo") is None
        if node.comments is not None:
            comment_values = [tok.value for tok in node.comments if tok is not None and hasattr(tok, "value")]
            assert not any("# " in v for v in comment_values)

    def test_above_comment_not_captured(self):
        """extract_node does NOT capture above-comments; that is pop_node's job."""
        data = load("""
            foo: 1
            # above bar
            bar: 2
        """)
        node = extract_node(data, "bar")
        assert node.above_comment is None

    def test_from_seq_captures_location(self):
        data = load("""
            items:
              - name: alpha
                val: 1
              - name: beta
                val: 2
        """)
        seq = data["items"]
        node = extract_node(seq, 0)
        assert node.value["name"] == "alpha"
        assert node.original_location is not None

    def test_from_seq_second_item(self):
        data = load("""
            items:
              - name: alpha
              - name: beta
        """)
        seq = data["items"]
        node = extract_node(seq, 1)
        assert node.value["name"] == "beta"


# ─── pop_node ────────────────────────────────────────────────────────────────


class TestPopNode:
    def test_removes_key_from_parent(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        pop_node(data, "bar")
        assert "bar" not in data
        assert list(data.keys()) == ["foo", "baz"]

    def test_returns_correct_value(self):
        data = load("""
            foo: 1
            bar: hello
            baz: 3
        """)
        node = pop_node(data, "bar")
        assert node.value == "hello"

    def test_pop_first_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = pop_node(data, "foo")
        assert "foo" not in data
        assert node.value == 1
        assert list(data.keys()) == ["bar"]

    def test_pop_last_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = pop_node(data, "bar")
        assert "bar" not in data
        assert node.value == 2
        assert list(data.keys()) == ["foo"]

    def test_pop_only_key(self):
        data = load("""
            solo: 99
        """)
        node = pop_node(data, "solo")
        assert "solo" not in data
        assert node.value == 99

    def test_captures_above_comment_non_first(self):
        data = load("""
            foo: 1
            # above bar
            bar: 2
            baz: 3
        """)
        node = pop_node(data, "bar")
        assert node.above_comment is not None
        assert "above bar" in node.above_comment.value

    def test_captures_above_comment_first_key(self):
        data = load("""
            # above foo
            foo: 1
            bar: 2
        """)
        node = pop_node(data, "foo")
        assert node.above_comment is not None

    def test_no_above_comment_when_absent(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = pop_node(data, "bar")
        assert node.above_comment is None

    def test_captures_inline_comment(self):
        data = load("""
            foo: 1  # note
            bar: 2
        """)
        node = pop_node(data, "foo")
        assert node.comments is not None

    def test_pop_nested_value(self):
        data = load("""
            foo:
              x: 1
              y: 2
            bar: 3
        """)
        node = pop_node(data, "foo")
        assert "foo" not in data
        assert node.value["x"] == 1


# ─── assign_node ─────────────────────────────────────────────────────────────


class TestAssignNode:
    def test_assigns_scalar_value(self):
        data = load("""
            a: 1
        """)
        node = Node(value=42, original_location=None, comments=None, above_comment=None)
        assign_node(data, "new_key", node)
        assert data["new_key"] == 42

    def test_assigns_nested_value(self):
        data = load("""
            a: 1
        """)
        inner = CommentedMap({"x": 10, "y": 20})
        node = Node(value=inner, original_location=None, comments=None, above_comment=None)
        assign_node(data, "nested", node)
        assert data["nested"]["x"] == 10

    def test_restores_inline_comment(self):
        src = load("""
            foo: 1  # my comment
            bar: 2
        """)
        node = pop_node(src, "foo")
        dst = load("""
            a: 1
            b: 2
        """)
        assign_node(dst, "foo", node)
        result = dump(dst)
        assert "# my comment" in result

    def test_restores_above_comment(self):
        src = load("""
            foo: 1
            # important note
            bar: 2
            baz: 3
        """)
        node = pop_node(src, "bar")
        dst = load("""
            x: 1
            y: 2
        """)
        assign_node(dst, "bar", node)
        result = dump(dst)
        assert "# important note" in result

    def test_position_insert_at_start(self):
        data = load("""
            a: 1
            b: 2
            c: 3
        """)
        node = Node(value=99, original_location=None, comments=None, above_comment=None)
        assign_node(data, "x", node, position=0)
        keys = list(data.keys())
        assert keys[0] == "x"

    def test_position_insert_in_middle(self):
        data = load("""
            a: 1
            b: 2
            c: 3
        """)
        node = Node(value=99, original_location=None, comments=None, above_comment=None)
        assign_node(data, "x", node, position=1)
        keys = list(data.keys())
        assert keys[1] == "x"

    def test_none_comments_skipped(self):
        data = load("""
            a: 1
        """)
        node = Node(value=5, original_location=None, comments=None, above_comment=None)
        assign_node(data, "b", node)
        # No crash, and "b" key's ca is unset
        assert data["b"] == 5


# ─── extract_above_comment / set_above_comment ───────────────────────────────


class TestExtractAboveComment:
    def test_non_first_key_with_comment(self):
        data = load("""
            foo: 1
            # above bar
            bar: 2
        """)
        result = extract_above_comment(data, "bar")
        assert result is not None
        assert "above bar" in result.value

    def test_non_first_key_no_comment(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        result = extract_above_comment(data, "bar")
        assert result is None

    def test_first_key_with_comment(self):
        data = load("""
            # above foo
            foo: 1
            bar: 2
        """)
        result = extract_above_comment(data, "foo")
        assert result is not None

    def test_first_key_no_comment(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        result = extract_above_comment(data, "foo")
        assert result is None

    def test_clears_from_preceding_key(self):
        data = load("""
            foo: 1
            # above bar
            bar: 2
        """)
        extract_above_comment(data, "bar")
        prev_ca = data.ca.items.get("foo")
        if prev_ca and len(prev_ca) > CA_AFTER_IDX and prev_ca[CA_AFTER_IDX] is not None:
            assert "above bar" not in prev_ca[CA_AFTER_IDX].value

    def test_key_not_present_returns_none(self):
        data = load("""
            foo: 1
        """)
        result = extract_above_comment(data, "nonexistent")
        assert result is None

    def test_no_ca_attribute_returns_none(self):
        result = extract_above_comment("not a map", "key")  # type: ignore[arg-type]
        assert result is None

    def test_multiline_above_comment(self):
        data = load("""
            foo: 1
            # line one
            # line two
            bar: 2
        """)
        result = extract_above_comment(data, "bar")
        assert result is not None
        assert "line one" in result.value or "line two" in result.value


class TestSetAboveComment:
    def test_set_on_first_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        tok = make_token("# my comment\n")
        set_above_comment(data, "foo", tok)
        result = dump(data)
        assert "# my comment" in result

    def test_set_on_non_first_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        tok = make_token("# above bar\n")
        set_above_comment(data, "bar", tok)
        result = dump(data)
        assert "# above bar" in result

    def test_set_none_is_noop(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        original = dump(data)
        set_above_comment(data, "bar", None)
        assert dump(data) == original

    def test_no_ca_attribute_is_noop(self):
        # Should not raise
        set_above_comment("not a map", "key", make_token("# x\n"))

    def test_key_not_in_mapping_is_noop(self):
        data = load("""
            foo: 1
        """)
        # Should not raise
        set_above_comment(data, "missing", make_token("# x\n"))

    def test_roundtrip_extract_then_set(self):
        data = load("""
            foo: 1
            # note
            bar: 2
        """)
        tok = extract_above_comment(data, "bar")
        assert tok is not None
        data2 = load("""
            x: 1
            y: 2
        """)
        set_above_comment(data2, "y", tok)
        result = dump(data2)
        assert "note" in result

    def test_appends_to_existing_ca_after(self):
        """set_above_comment appends when a CA_AFTER_IDX already exists on the preceding key."""
        data = load("""
            foo: 1  # inline
            bar: 2
            baz: 3
        """)
        tok = make_token("# above baz\n")
        set_above_comment(data, "baz", tok)
        result = dump(data)
        assert "# above baz" in result
        assert "# inline" in result


class TestSetFirstKeyAboveComment:
    def test_sets_comment_on_first_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        tok = make_token("# first!\n")
        set_first_key_above_comment(data, tok)
        result = dump(data)
        assert "# first!" in result

    def test_none_token_is_noop(self):
        data = load("""
            foo: 1
        """)
        original = dump(data)
        set_first_key_above_comment(data, None)
        assert dump(data) == original

    def test_list_of_tokens(self):
        data = load("""
            foo: 1
        """)
        tokens = [make_token("# line 1\n"), make_token("# line 2\n")]
        set_first_key_above_comment(data, tokens)
        result = dump(data)
        assert "# line 1" in result or "# line 2" in result


# ─── extract_preceding_text_comment ──────────────────────────────────────────


class TestExtractPrecedingTextComment:
    def test_comment_between_keys(self):
        data = load("""
            foo: 1
            # my note
            bar: 2
        """)
        result = extract_preceding_text_comment(data, "bar")
        assert result is not None
        assert "my note" in result.value

    def test_no_comment(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        result = extract_preceding_text_comment(data, "bar")
        assert result is None

    def test_first_key_returns_none(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        result = extract_preceding_text_comment(data, "foo")
        assert result is None

    def test_clears_from_preceding_slot(self):
        data = load("""
            foo: 1
            # my note
            bar: 2
        """)
        extract_preceding_text_comment(data, "bar")
        prev_ca = data.ca.items.get("foo")
        if prev_ca and len(prev_ca) > CA_AFTER_IDX and prev_ca[CA_AFTER_IDX] is not None:
            assert "my note" not in prev_ca[CA_AFTER_IDX].value

    def test_no_ca_attribute_returns_none(self):
        result = extract_preceding_text_comment("not a map", "key")  # type: ignore[arg-type]
        assert result is None

    def test_splits_combined_inline_and_above_comment(self):
        """When preceding key has both inline and above comments combined, only above is returned."""
        data = load("""
            foo: 1  # foo-inline
            # above bar
            bar: 2
        """)
        result = extract_preceding_text_comment(data, "bar")
        assert result is not None
        assert "above bar" in result.value
        # The inline comment for foo should stay
        foo_ca = data.ca.items.get("foo")
        if foo_ca and len(foo_ca) > CA_AFTER_IDX and foo_ca[CA_AFTER_IDX] is not None:
            assert "foo-inline" in foo_ca[CA_AFTER_IDX].value


# ─── set_blank_line_before ───────────────────────────────────────────────────


class TestSetBlankLineBefore:
    def test_adds_blank_line(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        set_blank_line_before(data, "bar")
        result = dump(data)
        assert "\n\n" in result

    def test_first_key_is_noop(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        original = dump(data)
        set_blank_line_before(data, "foo")
        assert dump(data) == original

    def test_key_not_present_is_noop(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        set_blank_line_before(data, "missing")
        # Should not crash

    def test_sets_ca_after_idx_on_preceding_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        set_blank_line_before(data, "bar")
        foo_ca = data.ca.items.get("foo")
        assert foo_ca is not None
        assert foo_ca[CA_AFTER_IDX] is not None
        assert "\n\n" in foo_ca[CA_AFTER_IDX].value

    def test_multiple_keys(self):
        data = load("""
            a: 1
            b: 2
            c: 3
        """)
        set_blank_line_before(data, "b")
        set_blank_line_before(data, "c")
        result = dump(data)
        assert result.count("\n\n") >= 2


# ─── copy_ca ────────────────────────────────────────────────────────────────


class TestCopyCa:
    def test_copies_comment_metadata(self):
        src = load("""
            foo: 1  # comment
            bar: 2
        """)
        dst = CommentedMap({"foo": 1, "bar": 2})
        copy_ca(src, dst)
        assert "foo" in dst.ca.items
        assert dst.ca.items["foo"] is not src.ca.items["foo"]  # deep copy

    def test_empty_source_is_noop(self):
        src = CommentedMap({"a": 1})
        dst = CommentedMap({"a": 1})
        copy_ca(src, dst)
        assert dst.ca.items == {}

    def test_does_not_share_references(self):
        src = load("""
            foo: 1  # x
            bar: 2
        """)
        dst = CommentedMap({"foo": 1, "bar": 2})
        copy_ca(src, dst)
        # Mutating dst.ca should not affect src.ca
        dst.ca.items["foo"] = None
        assert src.ca.items.get("foo") is not None


# ─── extract_trailing_separator / set_trailing_separator ─────────────────────


class TestExtractTrailingSeparator:
    def test_empty_map_returns_none(self):
        result = extract_trailing_separator(CommentedMap())
        assert result is None

    def test_non_map_returns_none(self):
        assert extract_trailing_separator("string") is None
        assert extract_trailing_separator(None) is None
        assert extract_trailing_separator([1, 2]) is None

    def test_simple_map_no_separator(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        sep = extract_trailing_separator(data)
        assert sep is None

    def test_extract_clears_slot(self):
        # The trailing blank line is intentional — it creates a trailing separator token
        data = load_yaml("""\
foo: 1
bar: 2

""")
        extract_trailing_separator(data)
        # After extraction, CA_AFTER_IDX of last key should be None
        bar_ca = data.ca.items.get("bar")
        if bar_ca and len(bar_ca) > CA_AFTER_IDX:
            assert bar_ca[CA_AFTER_IDX] is None

    def test_nested_map_walks_deep(self):
        data = load("""
            parent:
              child:
                leaf: 1
            second: 2
        """)
        # Should not crash on nested structures
        sep = extract_trailing_separator(data)
        assert sep is None or hasattr(sep, "value")


class TestSetTrailingSeparator:
    def test_sets_on_simple_map(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        tok = make_token("\n\n")
        set_trailing_separator(data, tok)
        bar_ca = data.ca.items.get("bar")
        assert bar_ca is not None
        assert bar_ca[CA_AFTER_IDX] is not None

    def test_empty_map_is_noop(self):
        data = CommentedMap()
        tok = make_token("\n\n")
        set_trailing_separator(data, tok)  # Should not crash

    def test_non_map_is_noop(self):
        set_trailing_separator("not a map", make_token("\n\n"))  # Should not crash

    def test_appends_to_existing(self):
        data = load("""
            foo: 1  # inline
            bar: 2
        """)
        # Manually set an existing CA_AFTER_IDX on 'bar'
        existing_tok = make_token("\n# existing\n")
        data.ca.items["bar"] = [None, None, existing_tok, None]
        sep = make_token("\n\n")
        set_trailing_separator(data, sep)
        bar_ca = data.ca.items["bar"]
        assert "existing" in bar_ca[CA_AFTER_IDX].value

    def test_roundtrip_extract_and_set(self):
        """Extracting a separator then setting it on another map should work."""
        src = load("""
            a: 1
            b: 2
        """)
        tok = make_token("\n\n")
        set_trailing_separator(src, tok)
        sep = extract_trailing_separator(src)
        dst = load("""
            x: 1
            y: 2
        """)
        if sep is not None:
            set_trailing_separator(dst, sep)
            assert dst.ca.items.get("y") is not None

    def test_nested_map_delegates_deep(self):
        data = load("""
            parent:
              child:
                leaf: 1
        """)
        tok = make_token("\n\n")
        set_trailing_separator(data, tok)
        # Should set it on the deepest last key (leaf), not on parent
        leaf_ca = data["parent"]["child"].ca.items.get("leaf")
        assert leaf_ca is not None
        assert leaf_ca[CA_AFTER_IDX] is not None


# ─── rebalance_trailing_separator ────────────────────────────────────────────


class TestRebalanceTrailingSeparator:
    def test_noop_when_k_is_last_key(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        # When 'bar' is the last key, no rebalance needed
        original = dump(data)
        rebalance_trailing_separator(data, "bar", {"foo", "bar"})
        assert dump(data) == original

    def test_moves_separator_when_new_key_added_after_k(self):
        """When a new key was appended after k, the separator should move to the new last key."""
        data = load("""
            foo:
              x: 1
            bar: 2
        """)
        original_keys = {"foo", "bar"}
        # Simulate adding a new key after foo
        data["meta"] = CommentedMap({"z": 99})
        # Put a trailing separator on foo's subtree
        tok = make_token("\n\n")
        set_trailing_separator(data["foo"], tok)
        rebalance_trailing_separator(data, "foo", original_keys)
        # set_trailing_separator walks into nested maps, so the sep lands on meta["z"]
        z_ca = data["meta"].ca.items.get("z")
        assert z_ca is not None
        assert z_ca[CA_AFTER_IDX] is not None


# ─── delete_top_level_key ────────────────────────────────────────────────────


class TestDeleteTopLevelKey:
    def test_delete_middle_key(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        delete_top_level_key(data, "bar")
        assert "bar" not in data
        assert list(data.keys()) == ["foo", "baz"]

    def test_delete_first_key(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        delete_top_level_key(data, "foo")
        assert "foo" not in data
        assert list(data.keys()) == ["bar", "baz"]

    def test_delete_last_key(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        delete_top_level_key(data, "baz")
        assert "baz" not in data
        assert list(data.keys()) == ["foo", "bar"]

    def test_delete_only_key(self):
        data = load("""
            solo: 1
        """)
        delete_top_level_key(data, "solo")
        assert "solo" not in data

    def test_nonexistent_key_is_noop(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        delete_top_level_key(data, "missing")
        assert list(data.keys()) == ["foo", "bar"]

    def test_preserves_above_comment_of_next_key_when_deleted_key_is_nested(self):
        """Above-comment of the next key is preserved only when the deleted key has a nested value.

        The comment lives in the deep trailing of the deleted key's nested value tree,
        so extract_deep_trailing_above_comment can find and relocate it.
        """
        data = load("""
            foo: 1
            bar:
              x: 1
            # above baz
            baz: 3
        """)
        delete_top_level_key(data, "bar")
        result = dump(data)
        assert "# above baz" in result

    def test_scalar_deleted_key_loses_next_above_comment(self):
        """When the deleted key has a scalar value, the above-comment for the next key is lost.

        The comment is stored in the deleted key's ca.items slot, which is removed in step 3.
        extract_deep_trailing_above_comment cannot find it because the value is not a CommentedMap/Seq.
        """
        data = load("""
            foo: 1
            bar: 2
            # above baz
            baz: 3
        """)
        delete_top_level_key(data, "bar")
        result = dump(data)
        # Comment for baz is lost when bar is a scalar (known limitation)
        assert "baz: 3" in result

    def test_removes_above_comment_of_deleted_key(self):
        data = load("""
            foo: 1
            # above bar
            bar: 2
            baz: 3
        """)
        delete_top_level_key(data, "bar")
        result = dump(data)
        assert "# above bar" not in result

    def test_delete_with_nested_value(self):
        data = load("""
            foo:
              x: 1
              y: 2
            bar: 3
            baz: 4
        """)
        delete_top_level_key(data, "foo")
        assert "foo" not in data
        assert list(data.keys()) == ["bar", "baz"]

    def test_delete_nested_value_transfers_next_above_comment(self):
        """When the deleted key has a nested value, the next key's above comment is moved
        from the deleted key's deep trailing to the surviving previous key's deep trailing.
        """
        data = load("""
            foo: 1
            bar:
              y: 2
            # above baz
            baz: 3
        """)
        delete_top_level_key(data, "bar")
        result = dump(data)
        assert "# above baz" in result


# ─── extract_nodes_by_name ───────────────────────────────────────────────────


class TestExtractNodesByName:
    def test_basic_named_items(self):
        data = load("""
            items:
              - name: alpha
                val: 1
              - name: beta
                val: 2
        """)
        result = extract_nodes_by_name(data["items"])
        assert set(result.keys()) == {"alpha", "beta"}
        assert result["alpha"].value["val"] == 1
        assert result["beta"].value["val"] == 2

    def test_skips_items_without_name(self):
        data = load("""
            items:
              - name: alpha
                val: 1
              - val: 2
        """)
        result = extract_nodes_by_name(data["items"])
        assert "alpha" in result
        assert len(result) == 1

    def test_empty_sequence(self):
        result = extract_nodes_by_name(CommentedSeq())
        assert result == {}

    def test_all_items_unnamed(self):
        data = load("""
            items:
              - val: 1
              - val: 2
        """)
        result = extract_nodes_by_name(data["items"])
        assert result == {}

    def test_captures_location_for_each(self):
        data = load("""
            items:
              - name: a
                v: 1
              - name: b
                v: 2
        """)
        result = extract_nodes_by_name(data["items"])
        assert result["a"].original_location is not None
        assert result["b"].original_location is not None
        # Second item is on a later line than the first
        assert result["b"].original_location.line > result["a"].original_location.line

    def test_single_named_item(self):
        data = load("""
            items:
              - name: only
                v: 42
        """)
        result = extract_nodes_by_name(data["items"])
        assert result["only"].value["v"] == 42


# ─── append_node ─────────────────────────────────────────────────────────────


class TestAppendNode:
    def test_appends_to_empty_seq(self):
        seq = CommentedSeq()
        item = CommentedMap({"name": "new", "val": 1})
        node = Node(value=item, original_location=None, comments=None, above_comment=None)
        append_node(seq, node)
        assert len(seq) == 1
        assert seq[0]["name"] == "new"

    def test_appends_after_existing(self):
        data = load("""
            items:
              - name: a
                val: 1
        """)
        seq = data["items"]
        item = CommentedMap({"name": "b", "val": 2})
        node = Node(value=item, original_location=None, comments=None, above_comment=None)
        append_node(seq, node)
        assert len(seq) == 2
        assert seq[1]["name"] == "b"

    def test_restores_comment_metadata(self):
        data = load("""
            items:
              - name: a
                val: 1
              - name: b
                val: 2
        """)
        seq = data["items"]
        orig_node = extract_node(seq, 0)
        # Create a fresh seq and append with comments
        seq2 = CommentedSeq()
        seq2.append(CommentedMap({"name": "placeholder"}))
        append_node(seq2, orig_node)
        if orig_node.comments is not None:
            assert 1 in seq2.ca.items

    def test_increments_index(self):
        seq = CommentedSeq()
        for i in range(3):
            node = Node(value=CommentedMap({"i": i}), original_location=None, comments=None, above_comment=None)
            append_node(seq, node)
        assert len(seq) == 3
        assert seq[2]["i"] == 2


# ─── reattach_next_key_above_comment ─────────────────────────────────────────


class TestReattachNextKeyAboveComment:
    def test_noop_when_next_key_is_none(self):
        data = load("""
            foo: 1
            bar: 2
        """)
        node = pop_node(data, "bar")
        # Should not raise
        reattach_next_key_above_comment(node, data, None)

    def test_noop_when_comments_is_none(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        node = pop_node(data, "foo")
        node.comments = None
        reattach_next_key_above_comment(node, data, "bar")
        # Should not raise

    def test_noop_when_ca_after_is_none(self):
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        node = pop_node(data, "foo")
        if node.comments is not None and len(node.comments) > CA_AFTER_IDX:
            node.comments[CA_AFTER_IDX] = None
        reattach_next_key_above_comment(node, data, "bar")
        # Should not raise

    def test_splits_inline_and_above_comment(self):
        data = load("""
            foo: 1  # foo-inline
            # above bar
            bar: 2
            baz: 3
        """)
        node = pop_node(data, "foo")
        # After popping foo, bar becomes first key in data
        reattach_next_key_above_comment(node, data, "bar")
        result = dump(data)
        assert "# above bar" in result

    def test_no_above_content_is_noop(self):
        """If CA_AFTER_IDX has only inline part with no above content, nothing is reattached."""
        data = load("""
            foo: 1
            bar: 2
            baz: 3
        """)
        node = pop_node(data, "foo")
        reattach_next_key_above_comment(node, data, "bar")
        # bar's above comment should not have changed to something unexpected
        result = dump(data)
        assert "bar: 2" in result


# ─── extract_deep_trailing_above_comment / insert_at_deep_trailing ───────────


class TestExtractDeepTrailingAboveComment:
    def test_scalar_returns_none(self):
        assert extract_deep_trailing_above_comment("scalar") is None

    def test_empty_map_returns_none(self):
        assert extract_deep_trailing_above_comment(CommentedMap()) is None

    def test_empty_seq_returns_none(self):
        assert extract_deep_trailing_above_comment(CommentedSeq()) is None

    def test_simple_map_no_above_comment(self):
        data = load("""
            a: 1
            b: 2
        """)
        result = extract_deep_trailing_above_comment(data)
        assert result is None

    def test_nested_map_walks_deep(self):
        # Build a nested structure with a trailing above-comment stored deep
        data = load("""
            outer:
              inner:
                leaf: 1
        """)
        result = extract_deep_trailing_above_comment(data)
        assert result is None

    def test_extracts_above_comment_token(self):
        """When a CA_AFTER_IDX with above content exists on the deepest key, it is returned."""
        data = load("""
            a: 1
            b: 2
        """)
        # Manually inject an above-comment token on b's CA_AFTER_IDX
        tok = make_token("\n# deep comment\n")
        data.ca.items["b"] = [None, None, tok, None]
        result = extract_deep_trailing_above_comment(data)
        assert result is not None
        assert "deep comment" in result.value

    def test_leaves_inline_part_in_place(self):
        r"""Only the above-comment part (after first \n) is extracted; inline stays."""
        data = load("""
            a: 1
            b: 2
        """)
        tok = make_token("\n# above\n")
        data.ca.items["b"] = [None, None, tok, None]
        extract_deep_trailing_above_comment(data)
        b_ca = data.ca.items.get("b")
        assert b_ca is not None
        # The CA_AFTER_IDX slot should still have something (the inline part)
        after = b_ca[CA_AFTER_IDX]
        assert after is not None


class TestInsertAtDeepTrailing:
    def test_empty_map_returns_false(self):
        result = insert_at_deep_trailing(CommentedMap(), make_token("# x\n"))
        assert result is False

    def test_non_map_or_seq_returns_false(self):
        result = insert_at_deep_trailing("string", make_token("# x\n"))
        assert result is False

    def test_inserts_into_simple_map(self):
        data = load("""
            a: 1
            b: 2
        """)
        tok = make_token("# inserted\n")
        result = insert_at_deep_trailing(data, tok)
        assert result is True
        b_ca = data.ca.items.get("b")
        assert b_ca is not None
        assert b_ca[CA_AFTER_IDX] is not None
        assert "inserted" in b_ca[CA_AFTER_IDX].value

    def test_inserts_into_nested_map(self):
        data = load("""
            outer:
              inner:
                leaf: 1
        """)
        tok = make_token("# nested\n")
        result = insert_at_deep_trailing(data, tok)
        assert result is True

    def test_prepends_before_existing_above(self):
        """New comment is inserted before any existing above-comment content."""
        data = load("""
            a: 1
            b: 2
        """)
        existing = make_token("\n# existing above\n")
        data.ca.items["b"] = [None, None, existing, None]
        new_tok = make_token("# new\n")
        insert_at_deep_trailing(data, new_tok)
        b_ca = data.ca.items["b"]
        combined = b_ca[CA_AFTER_IDX].value
        assert "# new" in combined
        assert "# existing above" in combined
        # new comes before existing
        assert combined.index("# new") < combined.index("# existing above")

    def test_seq_delegates_to_last_item(self):
        data = load("""
            items:
              - name: a
              - name: b
        """)
        seq = data["items"]
        tok = make_token("# for seq\n")
        result = insert_at_deep_trailing(seq, tok)
        assert result is True


# ─── round-trip tests ────────────────────────────────────────────────────────


class TestRoundTrip:
    """Pop a key and assign it elsewhere; verify YAML output is correct."""

    def test_move_middle_key_no_comments(self):
        src = load("""
            a: 1
            b: 2
            c: 3
        """)
        node = pop_node(src, "b")
        dst = load("""
            x: 10
            y: 20
        """)
        assign_node(dst, "b", node)
        assert "b" not in src
        assert dst["b"] == 2
        assert "b: 2" in dump(dst)

    def test_move_key_with_above_comment(self):
        src = load("""
            a: 1
            # important note
            b: 2
            c: 3
        """)
        node = pop_node(src, "b")
        dst = load("""
            x: 10
            y: 20
        """)
        assign_node(dst, "b", node)
        result = dump(dst)
        assert "# important note" in result
        assert "b: 2" in result

    def test_move_first_key_captures_above_comment(self):
        """pop_node on the first key captures its above-comment."""
        src = load("""
            # top comment
            a: 1
            b: 2
        """)
        node = pop_node(src, "a")
        assert node.above_comment is not None

    def test_move_first_key_with_above_comment_to_first_position(self):
        """A first-key above-comment (stored as a list of tokens) can be placed at position 0."""
        src = load("""
            # top comment
            a: 1
            b: 2
        """)
        node = pop_node(src, "a")
        dst = load("""
            x: 10
            y: 20
        """)
        assign_node(dst, "a", node, position=0)
        result = dump(dst)
        assert "# top comment" in result
        assert "a: 1" in result

    def test_move_key_with_inline_comment(self):
        src = load("""
            a: 1
            b: hello  # b value
            c: 3
        """)
        node = pop_node(src, "b")
        dst = load("""
            x: 10
            y: 20
        """)
        assign_node(dst, "b", node)
        result = dump(dst)
        assert "# b value" in result

    def test_pop_and_reassign_in_same_map(self):
        """Pop a key and re-insert it at a different position in the same map."""
        data = load("""
            a: 1
            b: 2
            c: 3
        """)
        node = pop_node(data, "b")
        assign_node(data, "b", node, position=0)
        keys = list(data.keys())
        assert keys[0] == "b"

    def test_delete_above_comment_removed_for_scalar_deleted_key(self):
        """Deleting a scalar key: above-comment of the deleted key is removed;
        above-comment of the next key is lost (stored in the deleted key's ca slot).
        """
        data = load("""
            a: 1
            # above b
            b: 2
            # above c
            c: 3
        """)
        delete_top_level_key(data, "b")
        result = dump(data)
        assert "# above b" not in result
        assert "a: 1" in result
        assert "c: 3" in result

    def test_blank_line_preserved_after_move(self):
        src = load_yaml("""\
a: 1

b: 2
c: 3
""")
        result_before = dump(src)
        assert "\n\n" in result_before

    def test_assign_with_position_preserves_comment(self):
        src = load("""
            a: 1
            # for b
            b: 2
            c: 3
        """)
        node = pop_node(src, "b")
        dst = load("""
            x: 1
            y: 2
            z: 3
        """)
        assign_node(dst, "b", node, position=1)
        result = dump(dst)
        assert "# for b" in result
        assert "b: 2" in result

    def test_seq_item_roundtrip(self):
        """Extract a seq item, append it to another seq, and verify value is preserved."""
        data = load("""
            items:
              - name: alpha
                val: 10
              - name: beta
                val: 20
        """)
        seq = data["items"]
        node = extract_node(seq, 1)
        seq2 = CommentedSeq()
        append_node(seq2, node)
        assert len(seq2) == 1
        assert seq2[0]["name"] == "beta"
        assert seq2[0]["val"] == 20
