"""Tests for resolve_env_vars."""

import pytest

from dbt_autofix.jinja import resolve_env_vars


@pytest.mark.parametrize(
    "env,raw,expected",
    [
        # Plain string without Jinja is returned unchanged
        ({}, "my_project", "my_project"),
        # Non-string values pass through
        ({}, None, None),
        ({}, 123, 123),
        # env_var present resolves to its value
        ({"DBT_AUTOFIX_TEST_NAME": "my_project"}, "{{ env_var('DBT_AUTOFIX_TEST_NAME') }}", "my_project"),
        # env_var missing with no default falls back to the raw string
        ({}, "{{ env_var('DBT_AUTOFIX_TEST_MISSING') }}", "{{ env_var('DBT_AUTOFIX_TEST_MISSING') }}"),
        # env_var missing with a default resolves to the default
        ({}, "{{ env_var('DBT_AUTOFIX_TEST_MISSING', 'fallback') }}", "fallback"),
        # Secret env vars are never resolved, even when set
        ({"DBT_ENV_SECRET_TOKEN": "shh"}, "{{ env_var('DBT_ENV_SECRET_TOKEN') }}", "{{ env_var('DBT_ENV_SECRET_TOKEN') }}"),
        # Private env vars are never resolved, even when set
        ({"DBT_ENV_PRIVATE_X": "hidden"}, "{{ env_var('DBT_ENV_PRIVATE_X') }}", "{{ env_var('DBT_ENV_PRIVATE_X') }}"),
        # Malformed Jinja falls back to the raw string
        ({}, "{{ env_var('X' }}", "{{ env_var('X' }}"),
    ],
)
def test_resolve_env_vars(env, raw, expected, monkeypatch):
    monkeypatch.delenv("DBT_AUTOFIX_TEST_MISSING", raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert resolve_env_vars(raw) == expected
