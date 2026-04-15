"""Unit tests for SAO cron utilities and YAML changeset."""
import pytest

from dbt_autofix.sao import cron_to_build_after, _build_after_minutes
from dbt_autofix.refactors.changesets.sao_yml import changeset_add_sao_config


class TestCronToBuildAfter:
    def test_every_12_hours(self):
        result = cron_to_build_after("9 */12 * * *")
        assert result == {"count": 12, "period": "hour"}

    def test_every_30_minutes(self):
        result = cron_to_build_after("*/30 * * * *")
        assert result == {"count": 30, "period": "minute"}

    def test_daily_midnight(self):
        result = cron_to_build_after("0 0 * * *")
        assert result == {"count": 24, "period": "hour"}

    def test_daily_fixed_time(self):
        result = cron_to_build_after("0 9 * * *")
        assert result == {"count": 24, "period": "hour"}

    def test_every_6_hours(self):
        result = cron_to_build_after("0 */6 * * *")
        assert result == {"count": 6, "period": "hour"}

    def test_invalid_cron_fallback(self):
        result = cron_to_build_after("not a cron")
        assert result["period"] in ("hour", "minute", "day")


class TestBuildAfterMinutes:
    def test_minutes(self):
        assert _build_after_minutes({"count": 30, "period": "minute"}) == 30.0

    def test_hours(self):
        assert _build_after_minutes({"count": 12, "period": "hour"}) == 720.0

    def test_days(self):
        assert _build_after_minutes({"count": 1, "period": "day"}) == 1440.0


class TestChangesetAddSaoConfig:
    def test_injects_build_after(self):
        yml = """
models:
  - name: my_model
    description: A model
""".lstrip()
        configs = {"my_model": {"count": 12, "period": "hour"}}
        result, changed = changeset_add_sao_config(yml, configs)
        assert changed
        assert "build_after" in result
        assert "count: 12" in result
        assert "period: hour" in result
        assert "updates_on: all" in result

    def test_skips_model_not_in_configs(self):
        yml = """
models:
  - name: other_model
    description: Not in SAO
""".lstrip()
        result, changed = changeset_add_sao_config(yml, {"my_model": {"count": 12, "period": "hour"}})
        assert not changed
        assert result == yml

    def test_skips_already_configured(self):
        yml = """
models:
  - name: my_model
    config:
      freshness:
        build_after:
          count: 6
          period: hour
""".lstrip()
        result, changed = changeset_add_sao_config(yml, {"my_model": {"count": 12, "period": "hour"}})
        assert not changed

    def test_config_inserted_after_name(self):
        yml = """
models:
  - name: my_model
    description: A model
    columns:
      - name: id
""".lstrip()
        configs = {"my_model": {"count": 24, "period": "hour"}}
        result, changed = changeset_add_sao_config(yml, configs)
        assert changed
        lines = result.splitlines()
        name_idx = next(i for i, l in enumerate(lines) if "name: my_model" in l)
        config_idx = next(i for i, l in enumerate(lines) if "config:" in l)
        assert config_idx == name_idx + 1

    def test_no_models_key(self):
        yml = "version: 2\n"
        result, changed = changeset_add_sao_config(yml, {"my_model": {"count": 12, "period": "hour"}})
        assert not changed

    def test_existing_config_gets_freshness_added(self):
        yml = """
models:
  - name: my_model
    config:
      tags:
        - daily
""".lstrip()
        configs = {"my_model": {"count": 12, "period": "hour"}}
        result, changed = changeset_add_sao_config(yml, configs)
        assert changed
        assert "freshness" in result
