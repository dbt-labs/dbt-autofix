from dbt_autofix.retrieve_schemas import SchemaSpecs


def test_dict_config_analysis_ignores_boolean_schema_references(monkeypatch):
    schema = {
        "definitions": {
            "ProjectModelConfig": {
                "properties": {
                    "+lifetime": {"anyOf": [{"$ref": "#/definitions/AnyValue"}, {"type": "null"}]},
                    "+persist_docs": {"anyOf": [{"$ref": "#/definitions/PersistDocs"}, {"type": "null"}]},
                }
            },
            "AnyValue": True,
            "PersistDocs": {
                "type": "object",
                "properties": {"columns": {"type": "boolean"}, "relation": {"type": "boolean"}},
            },
        }
    }
    schema_specs = object.__new__(SchemaSpecs)
    schema_specs._dict_config_cache = None
    schema_specs._schema_version = "test"
    schema_specs.client = object()
    monkeypatch.setattr(
        "dbt_autofix.retrieve_schemas.get_fusion_dbt_project_schema",
        lambda client, version: schema,
    )

    analysis = schema_specs.get_dict_config_analysis()

    assert analysis == {
        "specific_properties": {"persist_docs": {"columns", "relation"}},
        "open_ended": set(),
    }
