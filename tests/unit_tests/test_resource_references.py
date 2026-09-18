from importlib import import_module
from pathlib import Path

from dbt_autofix.refactors.changesets.resource_references import ResourceRenameMap
from dbt_autofix.refactors.results import SQLContent, SQLRefactorConfig
from dbt_autofix.retrieve_schemas import SchemaSpecs


class EmptySchemaSpecs(SchemaSpecs):
    def __init__(self):
        self.yaml_specs_per_node_type = {}
        self.dbtproject_specs_per_node_type = {}


def test_source_reference_is_updated_after_source_name_normalization():
    content = SQLContent(
        original_str="select * from {{ source('vendor data', 'upstream_view') }}",
        current_str="select * from {{ source('vendor data', 'upstream_view') }}",
        current_file_path=Path("consumer.sql"),
    )
    config = SQLRefactorConfig(schema_specs=EmptySchemaSpecs(), node_type="models")
    setattr(config, "resource_rename_map", ResourceRenameMap(source_renames={"vendor data": "vendor_data"}))

    resource_references = import_module("dbt_autofix.refactors.changesets.resource_references")
    result = resource_references.update_resource_references_sql(content, config)

    assert result.refactored
    assert "source('vendor_data', 'upstream_view')" in result.refactored_content
