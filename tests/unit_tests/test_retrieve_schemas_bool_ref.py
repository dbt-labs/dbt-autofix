"""Tests for SchemaSpecs.get_dict_config_analysis boolean $ref handling."""

from dbt_autofix.retrieve_schemas import SchemaSpecs


def test_get_dict_config_analysis_skips_boolean_ref_targets(monkeypatch):
    """Boolean JSON Schema subschemas (e.g. AnyValue: true) must not crash analysis.

    Fusion schema v2.0.0-preview.209 added +lifetime/+range/+update_lag whose
    anyOf $ref points at definitions.AnyValue = true. Calling .get on that bool
    raised AttributeError (dbt-labs/dbt-autofix#417).
    """
    schema = {
        "definitions": {
            "AnyValue": True,
            "ProjectModelConfig": {
                "properties": {
                    "+lifetime": {
                        "anyOf": [
                            {"$ref": "#/definitions/AnyValue"},
                            {"type": "null"},
                        ]
                    },
                    "+persist_docs": {
                        "type": ["object", "null"],
                        "properties": {"columns": {}, "relation": {}},
                    },
                    "+labels": {
                        "type": ["object", "null"],
                        "additionalProperties": True,
                    },
                }
            },
        }
    }

    specs = SchemaSpecs.__new__(SchemaSpecs)
    specs._dict_config_cache = None
    specs._schema_version = "test"

    monkeypatch.setattr(
        "dbt_autofix.retrieve_schemas.get_fusion_latest_version",
        lambda client: "test",
    )
    monkeypatch.setattr(
        "dbt_autofix.retrieve_schemas.get_fusion_dbt_project_schema",
        lambda client, version: schema,
    )
    specs.client = object()

    result = specs.get_dict_config_analysis()

    assert "persist_docs" in result["specific_properties"]
    assert result["specific_properties"]["persist_docs"] == {"columns", "relation"}
    assert "labels" in result["open_ended"]
    assert "lifetime" not in result["specific_properties"]
    assert "lifetime" not in result["open_ended"]
