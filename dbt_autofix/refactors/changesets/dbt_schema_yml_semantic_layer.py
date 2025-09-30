from typing import List, Tuple, Dict, Any
from dbt_autofix.refactors.results import YMLRuleRefactorResult
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str
from dbt_autofix.semantic_definitions import SemanticDefinitions


def changeset_merge_metrics_with_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for i, node in enumerate(yml_dict.get("models") or []):
        processed_node, node_refactored, node_refactor_logs = merge_metrics_with_model(
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
        rule_name="merge_metrics_with_model_metrics",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )


def merge_metrics_with_model(node: Dict[str, Any], semantic_definitions: SemanticDefinitions) -> Tuple[Dict[str, Any], bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []
    simple_metrics_on_model = {metric["name"]: metric for metric in node.get("metrics", []) if metric["type"] == "simple"}

    # For each top-level metric, determine whether it can be merged with the model depending on its linked measures
    for metric_name, metric in semantic_definitions.metrics.items():
        # No need to further merge metrics that have already been merged
        if metric_name in semantic_definitions.merged_metrics:
            continue

        # Simple metrics can be merged to this model if they have a measure that exists as a simple metric on the model
        if metric["type"] == "simple":
            # Extract measure name from top-level simple metric
            if isinstance(metric["type_params"]["measure"], dict):
                measure_name = metric["type_params"]["measure"]["name"]
            else:
                measure_name = metric["type_params"]["measure"]

            if measure_name in simple_metrics_on_model:
                metric_update = {}
                if "label" in metric:
                    metric_update["label"] = metric["label"]
                if "join_to_timespine" in metric:
                    metric_update["join_to_timespine"] = metric["join_to_timespine"]
                if "fill_nulls_with" in metric:
                    metric_update["fill_nulls_with"] = metric["fill_nulls_with"]
                if "filter" in metric:
                    metric_update["filter"] = metric["filter"]
                if "alias" in metric:
                    metric_update["alias"] = metric["alias"]

                simple_metrics_on_model[measure_name].update(metric_update)
                # Remove existing 'hidden' property if merging into existing simple metric
                simple_metrics_on_model[measure_name].pop("hidden", None)
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Merged simple metric '{metric_name}' with simple metric '{metric_name}' on model '{node['name']}'.")
        # Derived metrics can be merged to this model if they have metrics that exist as simple metrics on the model
        elif metric["type"] == "derived":
            metric_names = []
            for input_metric in metric.get("type_params", {}).get("metrics", []):
                if isinstance(input_metric, dict):
                    metric_names.append(input_metric["name"])
                else:
                    metric_names.append(input_metric)

            if all(metric_name in simple_metrics_on_model for metric_name in metric_names):
                # Remove type_params from top-level
                type_params = metric.pop("type_params", {})
                metric.update(type_params)
                # Rename "metrics" to "input_metrics"
                if "metrics" in metric:
                    metric["input_metrics"] = metric.pop("metrics")

                node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added derived metric '{metric_name}' with to model '{node['name']}'.")
        # Ratio metrics can be merged to this model if they have numerator and denominator that exist as simple metrics on the model
        elif metric["type"] == "ratio":
            numerator = metric.get("type_params", {}).get("numerator")
            if isinstance(numerator, dict):
                numerator_name = numerator["name"]
            else:
                numerator_name = numerator
            
            denominator = metric.get("type_params", {}).get("denominator")
            if isinstance(denominator, dict):
                denominator_name = denominator["name"]
            else:
                denominator_name = denominator

            if numerator_name in simple_metrics_on_model and denominator_name in simple_metrics_on_model:
                # Remove type_params from top-level
                type_params = metric.pop("type_params", {})
                metric.update(type_params)

                node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added ratio metric '{metric_name}' to model '{node['name']}'.")
        
        elif metric["type"] == "cumulative":
            measure = metric.get("type_params", {}).get("measure")
            if isinstance(measure, dict):
                measure_name = measure["name"]
            else:
                measure_name = measure

            if measure_name in simple_metrics_on_model:
                # Remove type_params from top-level
                type_params = metric.pop("type_params", {})
                metric.update(type_params)

                # Rename "measure" to "input_metric"
                if "measure" in metric:
                    metric["input_metric"] = metric.pop("measure")
                    # Remove "fill_nulls_with" and "join_to_timespine" from input_metrics
                    if isinstance(metric["input_metric"], dict):
                        metric["input_metric"].pop("fill_nulls_with", None)
                        metric["input_metric"].pop("join_to_timespine", None)

                node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added cumulative metric '{metric_name}' to model '{node['name']}'.")
        elif metric["type"] == "conversion":
            base_measure = metric.get("type_params", {}).get("conversion_type_params", {}).get("base_measure")
            if isinstance(base_measure, dict):
                base_measure_name = base_measure["name"]
            else:
                base_measure_name = base_measure
            
            conversion_measure = metric.get("type_params", {}).get("conversion_type_params", {}).get("conversion_measure")
            if isinstance(conversion_measure, dict):
                conversion_measure_name = conversion_measure["name"]
            else:
                conversion_measure_name = conversion_measure
            
            if base_measure_name in simple_metrics_on_model and conversion_measure_name in simple_metrics_on_model:
                # Remove type_params from top-level
                conversion_type_params = metric.pop("type_params", {}).pop("conversion_type_params", {})
                metric.update(conversion_type_params)

                # Rename "base_measure" to "base_metric"
                if "base_measure" in metric:
                    metric["base_metric"] = metric.pop("base_measure")
                    # Remove "fill_nulls_with" and "join_to_timespine" from base_metric
                    if isinstance(metric["base_metric"], dict):
                        metric["base_metric"].pop("fill_nulls_with", None)
                        metric["base_metric"].pop("join_to_timespine", None)
                # Rename "conversion_measure" to "conversion_metric"
                if "conversion_measure" in metric:
                    metric["conversion_metric"] = metric.pop("conversion_measure")
                    # Remove "fill_nulls_with" and "join_to_timespine" from conversion_metric
                    if isinstance(metric["conversion_metric"], dict):
                        metric["conversion_metric"].pop("fill_nulls_with", None)
                        metric["conversion_metric"].pop("join_to_timespine", None)

                node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added conversion metric '{metric_name}' to model '{node['name']}'.")

    return node, refactored, refactor_logs


def changeset_merge_semantic_models_with_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    # Merge semantic models with existing models in yml
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

    # Create new model entries for semantic models that don't have a corresponding model entry in any .yml file
    # and merge semantic models with them
    for semantic_model in yml_dict.get("semantic_models", []):
        model_key = semantic_definitions.get_model_key_for_semantic_model(semantic_model)
        # Semantic model has valid model 'ref' but no corresponding model entry in any .yml file
        if model_key and not semantic_definitions.model_key_exists_for_semantic_model(model_key):
            if "models" not in yml_dict:
                yml_dict["models"] = []

            new_model_node = {
                "name": model_key[0],
            }

            processed_new_model_node, new_model_node_refactored, new_model_node_refactor_logs = merge_semantic_models_with_model(
                new_model_node,
                semantic_definitions
            )
            if new_model_node_refactored:
                refactored = True
                yml_dict["models"].append(processed_new_model_node)
                for log in new_model_node_refactor_logs:
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

            # propagate measures to model metrics
            node_logs.extend(merge_measures_with_model_metrics(node, semantic_model.get("measures", [])))

            refactored = True
            refactor_log = f"Model '{node['name']}' - Merged with semantic model '{semantic_model['name']}'."
            semantic_definitions.mark_semantic_model_as_merged(semantic_model["name"])
            for log in node_logs:
                refactor_log += f"\n\t* {log}"
            refactor_logs.append(
                refactor_log
            )
        
    return node, refactored, refactor_logs

def merge_entities_with_model_columns(node: Dict[str, Any], entities: List[Dict[str, Any]]) -> List[str]:
    logs: List[str] = []
    node_columns = {column["name"]: column for column in node.get("columns", [])}

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
    node_columns = {column["name"]: column for column in node.get("columns", [])}

    for dimension in dimensions:
        dimension_col_name = dimension["name"]
        dimension_time_granularity = dimension.get("type_params", {}).get("time_granularity")

        # Add dimension to column if column already exists
        if dimension_col_name in node_columns:
            node_columns[dimension_col_name]["dimension"] = {
                "type": dimension["type"]
            }
            # Add time granularity to top-level column if it was defined on the dimension
            if dimension_time_granularity:
                node_columns[dimension_col_name]["granularity"] = dimension_time_granularity
            logs.append(f"Added '{dimension['type']}' dimension to column '{dimension_col_name}'.")
        # If column doesn't exist, add a new one with new dimension if no special characters in expr
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
            # Add time granularity to top-level column if it was defined on the dimension
            if dimension_time_granularity:
                node["columns"][-1]["granularity"] = dimension_time_granularity
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


def merge_measures_with_model_metrics(node: Dict[str, Any], measures: List[Dict[str, Any]]) -> List[str]:
    logs: List[str] = []
    node_metrics = {metric["name"]: metric for metric in node.get("metrics", [])}

    for measure in measures:
        metric_name = measure["name"]

        # Build metric to add to model / update existing metric on model
        metric = {
            "name": metric_name,
            "type": "simple",
            "label": measure.get("label") or metric_name
        }
        create_metric = measure.pop("create_metric", False)
        if not create_metric:
            metric["hidden"] = True

        for key, value in measure.items():
            metric[key] = value
        
        # Renamed non_additive_dimension keys
        if metric.get("non_additive_dimension"):
            # window_choice -> window_agg
            window_choice = metric["non_additive_dimension"].pop("window_choice", None)
            if window_choice:
                metric["non_additive_dimension"]["window_agg"] = window_choice
            # window_groupings -> group_by
            window_groupings = metric["non_additive_dimension"].pop("window_groupings", None)
            if window_groupings:
                metric["non_additive_dimension"]["group_by"] = window_groupings

        # Add measure to metric if metric already exists, or create new metric
        if metric_name in node_metrics:
            node_metrics[metric_name].update(metric)
            logs.append(f"Updated existing metric '{metric_name}' with measure '{metric_name}' from semantic model '{node['name']}'.")
        else:
            if "metrics" not in node:
                node["metrics"] = []
            node["metrics"].append(metric)
            logs.append(f"Added new simple metric '{metric_name}' from measure '{metric_name}' on semantic model '{node['name']}'.")
    
    return logs

def changeset_delete_top_level_semantic_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    top_level_semantic_models = yml_dict.get("semantic_models", [])
    new_semantic_models = []

    for semantic_model in top_level_semantic_models:
        if semantic_model["name"] in semantic_definitions.merged_semantic_models:
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Deleted top-level semantic model '{semantic_model['name']}'.",
                    deprecation=None
                )
            )
        else:
            new_semantic_models.append(semantic_model)
    
    if not new_semantic_models:
        yml_dict.pop("semantic_models", None)
    else:
        yml_dict["semantic_models"] = new_semantic_models

    return YMLRuleRefactorResult(
        rule_name="delete_top_level_semantic_models",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict, write_empty=True) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )


def changeset_migrate_or_delete_top_level_metrics(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    top_level_metrics = yml_dict.get("metrics", [])
    transformed_metrics = []

    for metric in top_level_metrics:
        # Do not include in transformed_metrics, effectively removing the metric from top-level specification
        if metric["name"] in semantic_definitions.merged_metrics:
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Deleted top-level metric '{metric['name']}'.",
                    deprecation=None
                )
            )
        else:
            # Transform metric to be compatible with new syntax, but leave metric at top-level
            if metric["type"] == "conversion":
                conversion_type_params = metric.pop("type_params", {}).pop("conversion_type_params", {})
                metric.update(conversion_type_params)
                # Rename "base_measure" to "base_metric"
                if "base_measure" in metric:
                    metric["base_metric"] = metric.pop("base_measure")
                # Rename "conversion_measure" to "conversion_metric"
                if "conversion_measure" in metric:
                    metric["conversion_metric"] = metric.pop("conversion_measure")
            else:
                # Bring type-params values to top-level
                type_params = metric.pop("type_params", {})
                metric.update(type_params)
                # Rename "metrics" to "input_metrics"
                if "metrics" in metric:
                    metric["input_metrics"] = metric.pop("metrics")

            transformed_metrics.append(metric)
            refactored = True
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f"Updated top-level metric '{metric['name']}' to be compatible with new syntax, but left at top-level.",
                    deprecation=None
                )
            )

    if not transformed_metrics:
        yml_dict.pop("metrics", None)
    else:
        yml_dict["metrics"] = transformed_metrics

    return YMLRuleRefactorResult(
        rule_name="migrate_or_delete_top_level_metrics",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict, write_empty=True) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors
    )