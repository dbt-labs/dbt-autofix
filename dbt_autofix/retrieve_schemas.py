import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

import httpx


@dataclass
class YAMLSpecs:
    allowed_config_fields: set[str]
    allowed_properties: set[str]

    def __post_init__(self):
        self.allowed_config_fields_without_meta = self.allowed_config_fields - {"meta"}


@dataclass
class DbtProjectSpecs:
    allowed_config_fields_dbt_project_with_plus: set[str]

    def __post_init__(self):
        self.allowed_config_fields_dbt_project = set(
            [conf[1:] for conf in self.allowed_config_fields_dbt_project_with_plus]
        )


class SchemaSpecs:
    def __init__(self, version: Optional[str] = None):
        self.yaml_specs_per_node_type, self.dbtproject_specs_per_node_type, self.valid_top_level_yaml_fields = self._get_specs(version)
        self.owner_properties = ["name", "email"]
        self.nodes_with_owner = ["groups", "exposures"]

    def _get_specs(self, version: Optional[str] = None) -> tuple[dict[str, YAMLSpecs], dict[str, DbtProjectSpecs], list[str]]:
        if os.getenv("DEBUG"):
            logging.basicConfig(level=logging.INFO)

        if version is None:
            version = get_fusion_latest_version()
        yml_schema = get_fusion_yml_schema(version)
        dbt_project_schema = get_fusion_dbt_project_schema(version)

        valid_top_level_yaml_fields = list(yml_schema["properties"].keys())

        node_type_to_config_key_aliases = {
            "models": ["dataset", "project", "data_space"],
            "seeds": ["dataset", "project", "data_space"],
            "snapshots": ["dataset", "project", "data_space"],
            "tests": ["dataset", "project", "data_space"],
            "sources": ["dataset", "project", "data_space"],
        }

        node_type_to_config_key_aliases_with_plus = {node_type: [f"+{alias}" for alias in aliases] for node_type, aliases in node_type_to_config_key_aliases.items()}

        # "models"
        model_property_field_name, model_config_field_name = self._get_yml_schema_fields(yml_schema, "models")
        yaml_specs_models = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][model_config_field_name]["properties"]).union(node_type_to_config_key_aliases["models"]),
            allowed_properties=set(yml_schema["definitions"][model_property_field_name]["properties"]),
        )
        model_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "models")
        dbtproject_specs_models = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][model_property_field_name_dbt_project]["properties"],
            ).union(node_type_to_config_key_aliases_with_plus["models"]),
        )

        # "sources"
        source_property_field_name, source_config_field_name = self._get_yml_schema_fields(yml_schema, "sources")
        yaml_specs_sources = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][source_config_field_name]["properties"]).union(node_type_to_config_key_aliases["sources"]),
            allowed_properties=set(yml_schema["definitions"][source_property_field_name]["properties"]),
        )
        source_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "sources")
        dbtproject_specs_sources = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][source_property_field_name_dbt_project]["properties"]
            ).union(node_type_to_config_key_aliases_with_plus["sources"]),
        )

        # "snapshots"
        snapshot_property_field_name, snapshot_config_field_name = self._get_yml_schema_fields(yml_schema, "snapshots")
        yaml_specs_snapshots = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][snapshot_config_field_name]["properties"]).union(node_type_to_config_key_aliases["snapshots"]),
            allowed_properties=set(yml_schema["definitions"][snapshot_property_field_name]["properties"]),
        )
        snapshot_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "snapshots")
        dbtproject_specs_snapshots = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][snapshot_property_field_name_dbt_project]["properties"]
            ).union(node_type_to_config_key_aliases_with_plus["snapshots"]),
        )

        # "seeds"
        seed_property_field_name, seed_config_field_name = self._get_yml_schema_fields(yml_schema, "seeds")
        yaml_specs_seeds = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][seed_config_field_name]["properties"]).union(node_type_to_config_key_aliases["seeds"]),
            allowed_properties=set(yml_schema["definitions"][seed_property_field_name]["properties"]),
        )
        seed_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "seeds")
        dbtproject_specs_seeds = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][seed_property_field_name_dbt_project]["properties"]
            ).union(node_type_to_config_key_aliases_with_plus["seeds"]),
        )

        # "exposures"
        exposure_property_field_name, exposure_config_field_name = self._get_yml_schema_fields(yml_schema, "exposures")
        yaml_specs_exposures = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][exposure_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][exposure_property_field_name]["properties"]),
        )
        exposure_property_field_name_dbt_project = self._get_dbt_project_schema_fields(
            dbt_project_schema, "exposures"
        )
        dbtproject_specs_exposures = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][exposure_property_field_name_dbt_project]["properties"]
            ),
        )

        # "tables"
        table_property_field_name, table_config_field_name = self._get_yml_schema_subfields(
            yml_schema, source_property_field_name, "tables"
        )
        yaml_specs_tables = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][table_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][table_property_field_name]["properties"]),
        )

        # "columns"
        column_property_field_name, column_config_field_name = self._get_yml_schema_subfields(
            yml_schema, model_property_field_name, "columns"
        )
        columns = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][column_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][column_property_field_name]["properties"]),
        )

        # "tests" or "data_tests"
        test_property_field_name, test_config_field_name = self._get_yml_schema_fields(yml_schema, "tests")
        yaml_specs_tests = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][test_config_field_name]["properties"]).union(node_type_to_config_key_aliases["tests"]),
            allowed_properties=set(yml_schema["definitions"][test_property_field_name]["properties"]),
        )
        test_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "tests")
        dbtproject_specs_tests = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][test_property_field_name_dbt_project]["properties"]
            ).union(node_type_to_config_key_aliases_with_plus["tests"]),
        )

        # "groups"
        group_property_field_name, group_config_field_name = self._get_yml_schema_fields(yml_schema, "groups")
        yaml_specs_groups = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][group_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][group_property_field_name]["properties"]),
        )

        # "analyses"
        analysis_property_field_name, analysis_config_field_name = self._get_yml_schema_fields(yml_schema, "analyses")
        yaml_specs_analyses = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][analysis_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][analysis_property_field_name]["properties"]),
        )

        # "unit_tests"
        unit_tests_property_field_name, unit_tests_config_field_name = self._get_yml_schema_fields(yml_schema, "unit_tests")
        yaml_specs_unit_tests = YAMLSpecs(
            allowed_config_fields=set(yml_schema["definitions"][unit_tests_config_field_name]["properties"]),
            allowed_properties=set(yml_schema["definitions"][unit_tests_property_field_name]["properties"]),
        )
        unit_tests_property_field_name_dbt_project = self._get_dbt_project_schema_fields(dbt_project_schema, "unit_tests")
        dbtproject_specs_unit_tests = DbtProjectSpecs(
            allowed_config_fields_dbt_project_with_plus=set(
                dbt_project_schema["definitions"][unit_tests_property_field_name_dbt_project]["properties"]
            ),
        )

        return (
            {
                "models": yaml_specs_models,
                "seeds": yaml_specs_seeds,
                "sources": yaml_specs_sources,
                "snapshots": yaml_specs_snapshots,
                "tests": yaml_specs_tests,  # tests can be at the top level, or nested under nodes or nested under nodes columns
                "data_tests": yaml_specs_tests,  # data_tests can be at the top level, or nested under nodes or nested under nodes columns
                "exposures": yaml_specs_exposures,
                "tables": yaml_specs_tables,  # tables is nested under sources
                "columns": columns,  # columns is nested under models
                "groups": yaml_specs_groups,
                "analyses": yaml_specs_analyses,
                "unit_tests": yaml_specs_unit_tests,
            },
            {
                "models": dbtproject_specs_models,
                "seeds": dbtproject_specs_seeds,
                # "semantic-models": dbtproject_specs_saved_queries, -- there is an issue with those specs in 165
                "snapshots": dbtproject_specs_snapshots,
                "sources": dbtproject_specs_sources,
                "tests": dbtproject_specs_tests,
                "data_tests": dbtproject_specs_tests,
                "exposures": dbtproject_specs_exposures,
                "unit-tests": dbtproject_specs_unit_tests,
            },
            valid_top_level_yaml_fields
        )

    def _get_yml_schema_fields(self, yml_schema: dict, node_type: str) -> tuple[str, str]:
        property_field_name = yml_schema["properties"][node_type]["items"]["$ref"].split("/")[-1]
        config_field_name = yml_schema["definitions"][property_field_name]["properties"]["config"]["anyOf"][0][
            "$ref"
        ].split("/")[-1]
        return property_field_name, config_field_name

    def _get_yml_schema_subfields(self, yml_schema: dict, definition: str, node_type: str) -> tuple[str, str]:
        property_field_name = yml_schema["definitions"][definition]["properties"][node_type]["items"]["$ref"].split(
            "/"
        )[-1]
        config_field_name = yml_schema["definitions"][property_field_name]["properties"]["config"]["anyOf"][0][
            "$ref"
        ].split("/")[-1]
        return property_field_name, config_field_name

    def _get_dbt_project_schema_fields(self, yml_schema: dict, node_type: str) -> str:
        property_field_name = yml_schema["properties"][node_type]["anyOf"][0]["$ref"].split("/")[-1]
        return property_field_name


def get_fusion_latest_version() -> str:
    latest_versions_url = "https://public.cdn.getdbt.com/fs/latest.json"
    resp = httpx.get(latest_versions_url)
    resp.raise_for_status()
    return resp.json()["tag"]


def get_fusion_yml_schema(version: str) -> dict:
    yml_schema_url = f"https://public.cdn.getdbt.com/fs/schemas/fs-schema-dbt-yaml-files-{version}.json"

    logging.info(f"Getting fusion yml schema for version {version}: {yml_schema_url}")
    response = httpx.get(yml_schema_url)
    response.raise_for_status()

    # for some reason we have 2 different schemas now in the response
    response_split = response.text.split("----------------------------------------------")
    
    return json.loads(response_split[-1])


def get_fusion_dbt_project_schema(version: str) -> dict:
    dbt_project_schema_url = f"https://public.cdn.getdbt.com/fs/schemas/fs-schema-dbt-project-{version}.json"

    logging.info(f"Getting fusion dbt project schema for version {version}: {dbt_project_schema_url}")
    response = httpx.get(dbt_project_schema_url)
    response.raise_for_status()

    return response.json()
