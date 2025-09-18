from typing import List, Tuple, Dict, Any
from dbt_autofix.refactors.results import YMLRuleRefactorResult
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str
from dbt_autofix.semantic_definitions import SemanticDefinitions
from dbt_autofix.deprecations import DeprecationType
from typing import Optional

def changeset_merge_semantic_models_with_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for i, node in enumerate(yml_dict.get("models") or []):
        processed_node, node_refactored, node_refactor_logs = merge_semantic_models_with_model(
            node,
            semantic_definitions
        )
        
        if node_refactored:
            refactored = True
            yml_dict["models"][i] = processed_node
            for log in node_refactor_logs:
                deprecation_refactors.append(
                    DbtDeprecationRefactor(
                        log=log,
                        deprecation=None
                    )
                )

    return YMLRuleRefactorResult(
        rule_name="restructure_owner_properties",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )


def merge_semantic_models_with_model(
    node: Dict[str, Any], semantic_definitions: SemanticDefinitions
) -> Tuple[Dict[str, Any], bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []

    if "versions" in node:
        # TODO: handle merging semantic models into versioned models
        pass
    else:
        if semantic_model := semantic_definitions.get_semantic_model(node["name"]):
            node_logs = []
            # Create a semantic_model property for the model
            semantic_model_block = {
                "enabled": True,
            }
            if semantic_model.get("config"):
                semantic_model_block["config"] = semantic_model["config"]
            if semantic_model["name"] != node["name"]:
                semantic_model_block["name"] = node["name"]
            node["semantic_model"] = semantic_model_block

            # Propagate semantic model properties to the model
            if semantic_model.get("description"):
                if node.get("description"):
                    node["description"] += node.get("description", "") + semantic_model["description"]
                    node_logs.append(f"Appended semantic model 'description' to model 'description'.")
                else:
                    node["description"] = semantic_model["description"]
                    node_logs.append(f"Set model 'description' to semantic model 'description'.")
            
            if agg_time_dimension := semantic_model.get("defaults", {}).get("agg_time_dimension"):
                node["agg_time_dimension"] = agg_time_dimension
                node_logs.append(f"Set model 'agg_time_dimension' to semantic model 'agg_time_dimension'.")
            
            # Propagate entities to model columns or derived_semantics
            node_logs.extend(merge_entities_with_model_columns(node, semantic_model.get("entities", [])))

            # Propagate dimensions to model columns or derived_semantics
            node_logs.extend(merge_dimensions_with_model_columns(node, semantic_model.get("dimensions", [])))

            refactored = True
            refactor_log = f"Model '{node['name']}' - Merged with semantic model '{semantic_model['name']}'."
            for log in node_logs:
                refactor_log += f"\n\t* {log}"
            refactor_logs.append(
                refactor_log
            )
        
    return node, refactored, refactor_logs

def merge_entities_with_model_columns(node: Dict[str, Any], entities: List[Dict[str, Any]]) -> List[str]:
    logs: List[str] = []
    node_columns = {column["name"]: column for column in node["columns"]}

    for entity in entities:
        entity_col_name = entity.get("expr") or entity["name"]

        # Add entity to column if column already exists
        if entity_col_name in node_columns:
            node_columns[entity_col_name]["entity"] = {
                "type": entity["type"]
            }
            if entity.get("name") != entity_col_name:
                node_columns[entity_col_name]["entity"]["name"] = entity["name"]
            logs.append(f"Added '{entity['type']}' entity to column '{entity_col_name}'.")
        # If column doesn't exist, add a new one with new entity if no special characters in expr
        elif not any(char in entity_col_name for char in (" ", "|", "(")):
            if node.get("columns"):
                node["columns"].append({
                    "name": entity_col_name,
                    "entity": {
                        "type": entity["type"]
                    }
                })
            else:
                node["columns"] = [{
                    "name": entity_col_name,
                    "entity": {
                        "type": entity["type"]
                    }
                }]
            logs.append(f"Added new column '{entity_col_name}' with '{entity['type']}' entity.")
        # Create entity as derived semantic entity
        else:
            if "derived_semantics" not in node:
                node["derived_semantics"] = {
                    "entities": []
                }
            
            if "entities" not in node["derived_semantics"]:
                node["derived_semantics"]["entities"] = []
            
            node["derived_semantics"]["entities"].append({
                "name": entity_col_name,
                "type": entity["type"],
            })
            if entity.get("expr"):
                node["derived_semantics"]["entities"][-1]["expr"] = entity["expr"]
            logs.append(f"Added 'derived_semantics' to model with '{entity['type']}' entity.")
    
    return logs

def merge_dimensions_with_model_columns(node: Dict[str, Any], dimensions: List[Dict[str, Any]]) -> List[str]:
    logs: List[str] = []
    node_columns = {column["name"]: column for column in node["columns"]}

    for dimension in dimensions:
        dimension_col_name = dimension["name"]
        dimension_time_granularity = dimension.get("type_params", {}).get("time_granularity")

        # Add dimension to column if column already exists
        if dimension_col_name in node_columns:
            node_columns[dimension_col_name]["dimension"] = {
                "type": dimension["type"]
            }
            if dimension_time_granularity:
                node_columns[dimension_col_name]["dimension"]["time_granularity"] = dimension_time_granularity
            logs.append(f"Added '{dimension['type']}' dimension to column '{dimension_col_name}'.")
        # If column doesn't exist, add a new one with new entity if no special characters in expr
        elif not any(char in dimension_col_name for char in (" ", "|", "(")):
            if node.get("columns"):
                node["columns"].append({
                    "name": dimension_col_name,
                    "dimension": {
                        "type": dimension["type"]
                    }
                })
            else:
                node["columns"] = [{
                    "name": dimension_col_name,
                    "dimension": {
                        "type": dimension["type"]
                    }
                }]
            if dimension_time_granularity:
                node["columns"][-1]["dimension"]["time_granularity"] = dimension_time_granularity
            logs.append(f"Added new column '{dimension_col_name}' with '{dimension['type']}' dimension.")
        # Create entity as derived semantic entity
        else:
            if "derived_semantics" not in node:
                node["derived_semantics"] = {
                    "entities": []
                }
            if "dimensions" not in node["derived_semantics"]:
                node["derived_semantics"]["dimensions"] = []
            
            node["derived_semantics"]["dimensions"].append({
                "name": dimension_col_name,
                "type": dimension["type"],
            })
            if dimension_time_granularity:
                node["derived_semantics"]["dimensions"][-1]["time_granularity"] = dimension_time_granularity
            logs.append(f"Added 'derived_semantics' to model with '{dimension['type']}' entity.")
    
    return logs


def changeset_delete_top_level_semantic_models(yml_str: str) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    if semantic_models_deleted := yml_dict.pop("semantic_models", None):
        refactored = True
        deprecation_refactors.append(
            DbtDeprecationRefactor(
                log="Deleted top-level 'semantic_models' definitions: " + ", ".join(["'" + semantic_model["name"] + "'" for semantic_model in semantic_models_deleted]),
                deprecation=None
            )
        )

    return YMLRuleRefactorResult(
        rule_name="delete_top_level_semantic_models",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )