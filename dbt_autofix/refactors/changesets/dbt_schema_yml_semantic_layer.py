from typing import List, Tuple, Dict, Any, Optional, Union, Callable
from dbt_autofix.refactors.results import YMLRuleRefactorResult
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str
from dbt_autofix.semantic_definitions import SemanticDefinitions


def changeset_merge_simple_metrics_with_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    return merge_metrics_with_models(yml_str, semantic_definitions, merge_simple_metrics_with_model)


def changeset_merge_complex_metrics_with_models(yml_str: str, semantic_definitions: SemanticDefinitions) -> YMLRuleRefactorResult:
    return merge_metrics_with_models(yml_str, semantic_definitions, merge_complex_metrics_with_model)


def merge_metrics_with_models(yml_str: str, semantic_definitions: SemanticDefinitions, merge_fn: Callable) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for i, node in enumerate(yml_dict.get("models") or []):
        processed_node, node_refactored, node_refactor_logs = merge_fn(
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


def merge_simple_metrics_with_model(node: Dict[str, Any], semantic_definitions: SemanticDefinitions) -> Tuple[Dict[str, Any], bool, List[str]]:
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
                new_measure_name = measure_name
                if "label" in metric:
                    metric_update["label"] = metric["label"]
                if "fill_nulls_with" in metric:
                    new_measure_name += f"_fill_nulls_with_{metric['fill_nulls_with']}"
                    metric_update["fill_nulls_with"] = metric["fill_nulls_with"]
                if "join_to_timespine" in metric:
                    new_measure_name += "_join_to_timespine"
                    metric_update["join_to_timespine"] = metric["join_to_timespine"]
                if "filter" in metric:
                    metric_update["filter"] = metric["filter"]
                if "alias" in metric:
                    metric_update["alias"] = metric["alias"]
                if new_measure_name != measure_name:
                    metric_update["name"] = new_measure_name

                # Apply update and rename simple metric if new_measure_name is different from measure_name
                simple_metrics_on_model[measure_name].update(metric_update)
                simple_metrics_on_model[new_measure_name] = simple_metrics_on_model.pop(measure_name)

                # Remove existing 'hidden' property if merging into existing simple metric
                simple_metrics_on_model[measure_name].pop("hidden", None)

                # Mark metric as refactored and merged
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Merged simple metric '{metric_name}' with simple metric '{metric_name}' on model '{node['name']}'.")
    
    return node, refactored, refactor_logs


def merge_complex_metrics_with_model(node: Dict[str, Any], semantic_definitions: SemanticDefinitions) -> Tuple[Dict[str, Any], bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []
    simple_metrics_on_model = {metric["name"]: metric for metric in node.get("metrics", []) if metric["type"] == "simple"}

    # For each top-level metric, determine whether it can be merged with the model depending on its linked measures
    for metric_name, metric in semantic_definitions.metrics.items():
        # No need to further merge metrics that have already been merged
        if metric_name in semantic_definitions.merged_metrics:
            continue
        # Derived metrics can be merged to this model if they have metrics that exist as simple metrics on the model
        if metric["type"] == "derived":
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
            raw_measure_name, measure_name = get_name_from_measure(measure)

            add_cumulative_metric_to_model = False
            if measure_name in simple_metrics_on_model:
                add_cumulative_metric_to_model = True                
            elif raw_measure_name and raw_measure_name in simple_metrics_on_model:
                add_cumulative_metric_to_model = True
                # Create hidden simple metric since only raw measure name is present
                new_simple_metric = create_hidden_simple_metric_from(simple_metrics_on_model[raw_measure_name], measure_name, fill_nulls_with, join_to_timespine)
                node["metrics"].append(new_simple_metric)
                refactored = True
                refactor_logs.append(f"Added hidden simple metric '{measure_name}' to model '{node['name']}'.")

            if add_cumulative_metric_to_model:
                node["metrics"].append(migrate_cumulative_metric(metric, measure_name))
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added cumulative metric '{metric_name}' to model '{node['name']}'.")
        
        elif metric["type"] == "conversion":
            base_measure = metric.get("type_params", {}).get("conversion_type_params", {}).get("base_measure")
            base_measure_fill_nulls_with = base_measure.get("fill_nulls_with") if isinstance(base_measure, dict) else None
            base_measure_join_to_timespine = base_measure.get("join_to_timespine") if isinstance(base_measure, dict) else None
            raw_base_measure_name, base_measure_name = get_name_from_measure(base_measure)
            
            conversion_measure = metric.get("type_params", {}).get("conversion_type_params", {}).get("conversion_measure")
            conversion_measure_fill_nulls_with = conversion_measure.get("fill_nulls_with") if isinstance(conversion_measure, dict) else None
            conversion_measure_join_to_timespine = conversion_measure.get("join_to_timespine") if isinstance(conversion_measure, dict) else None
            raw_conversion_measure_name, conversion_measure_name = get_name_from_measure(conversion_measure)

            add_conversion_metric_to_model = False
            add_hidden_base_metric_to_model = False
            add_hidden_conversion_metric_to_model = False
            if base_measure_name in simple_metrics_on_model and conversion_measure_name in simple_metrics_on_model:
                add_conversion_metric_to_model = True
            # Both base and conversion measures need simple metrics created
            elif raw_base_measure_name in simple_metrics_on_model and raw_conversion_measure_name in simple_metrics_on_model:
                add_conversion_metric_to_model = True
                add_hidden_base_metric_to_model = True
                add_hidden_conversion_metric_to_model = True
            # Only conversion measure needs a simple metric created
            elif raw_conversion_measure_name in simple_metrics_on_model and base_measure_name in simple_metrics_on_model:
                add_conversion_metric_to_model = True
                add_hidden_conversion_metric_to_model = True
            # Only base measure needs a simple metric created
            elif raw_base_measure_name in simple_metrics_on_model and conversion_measure_name in simple_metrics_on_model:
                add_conversion_metric_to_model = True
                add_hidden_base_metric_to_model = True

            if add_hidden_base_metric_to_model:
                new_simple_metric = create_hidden_simple_metric_from(simple_metrics_on_model[raw_base_measure_name], base_measure_name, base_measure_fill_nulls_with, base_measure_join_to_timespine)
                node["metrics"].append(new_simple_metric)
                refactored = True
                refactor_logs.append(f"Added hidden simple metric '{base_measure_name}' to model '{node['name']}'.")

            if add_hidden_conversion_metric_to_model:
                new_simple_metric = create_hidden_simple_metric_from(simple_metrics_on_model[raw_conversion_measure_name], conversion_measure_name, conversion_measure_fill_nulls_with, conversion_measure_join_to_timespine)
                node["metrics"].append(new_simple_metric)
                refactored = True
                refactor_logs.append(f"Added hidden simple metric '{conversion_measure_name}' to model '{node['name']}'.")

            if add_conversion_metric_to_model:
                node["metrics"].append(migrate_conversion_metric(metric, base_measure_name, conversion_measure_name))
                semantic_definitions.mark_metric_as_merged(metric_name)
                refactored = True
                refactor_logs.append(f"Added conversion metric '{metric_name}' to model '{node['name']}'.")

    return node, refactored, refactor_logs

def get_name_from_measure(measure: Union[str, Dict[str, Any]]) -> Tuple[str, str]:
    raw_measure_name, measure_name = None, None
    if isinstance(measure, dict):
        measure_name = measure["name"]
        raw_measure_name = measure_name
        # Update measure name with fill_nulls_with and join_to_timespine if provided
        fill_nulls_with = measure.get("fill_nulls_with")
        join_to_timespine = measure.get("join_to_timespine")
        if fill_nulls_with:
            measure_name += f"_fill_nulls_with_{fill_nulls_with}"
        if join_to_timespine:
            measure_name += "_join_to_timespine"
    else:
        raw_measure_name = measure
        measure_name = measure
    
    return raw_measure_name, measure_name

def create_hidden_simple_metric_from(other_simple_metric: Dict[str, Any], name: str, fill_nulls_with: Optional[str] = None, join_to_timespine: Optional[bool] = None) -> Dict[str, Any]:
    new_metric = other_simple_metric.copy()
    new_metric["name"] = name
    new_metric["hidden"] = True
    if fill_nulls_with:
        new_metric["fill_nulls_with"] = fill_nulls_with
    if join_to_timespine:
        new_metric["join_to_timespine"] = join_to_timespine

    return new_metric

def migrate_cumulative_metric(metric: Dict[str, Any], measure_name: str) -> Dict[str, Any]:
    # Remove type_params from top-level
    type_params = metric.pop("type_params", {})
    metric.update(type_params)

    # Rename "measure" to "input_metric"
    if "measure" in metric:
        metric["input_metric"] = metric.pop("measure")
    
    # Remove fill_nulls_with and join_to_timespine from input_metric if they exist
    if isinstance(metric["input_metric"], dict):
        metric["input_metric"].pop("fill_nulls_with", None)
        metric["input_metric"].pop("join_to_timespine", None)

    # Ensure cumulative metric is pointing to correct metric input
    if isinstance(metric["input_metric"], dict):
        metric["input_metric"]["name"] = measure_name
    else:
        metric["input_metric"] = measure_name

    return metric

def migrate_conversion_metric(metric: Dict[str, Any], base_measure_name: str, conversion_measure_name: str) -> Dict[str, Any]:
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
    
    # Ensure conversion metric is pointig to correct metric inputs
    if isinstance(metric["base_metric"], dict):
        metric["base_metric"]["name"] = base_measure_name
    else:
        metric["base_metric"] = base_measure_name

    if isinstance(metric["conversion_metric"], dict):
        metric["conversion_metric"]["name"] = conversion_measure_name
    else:
        metric["conversion_metric"] = conversion_measure_name
    
    return metric


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

    if semantic_model := semantic_definitions.get_semantic_model_for_model(node["name"]):
        node, node_logs = merge_semantic_model_with_model(semantic_model, node)
        refactored = True
        refactor_log = f"Model '{node['name']}' - Merged with semantic model '{semantic_model['name']}'."
        semantic_definitions.mark_semantic_model_as_merged(semantic_model["name"])
        for log in node_logs:
            refactor_log += f"\n\t* {log}"
        refactor_logs.append(
            refactor_log
        )
    elif "versions" in node:
        for version in node["versions"]:
            v = version["v"]
            if semantic_model := semantic_definitions.get_semantic_model_for_model(node["name"], v):
                node, node_logs = merge_semantic_model_with_model(semantic_model, node, v=v)
                refactored = True
                refactor_log = f"Model '{node['name']}' version '{v}' - Merged with semantic model '{semantic_model['name']}'."
                semantic_definitions.mark_semantic_model_as_merged(semantic_model["name"])
                for log in node_logs:
                    refactor_log += f"\n\t* {log}"
                refactor_logs.append(refactor_log)
        
    return node, refactored, refactor_logs

def merge_semantic_model_with_model(semantic_model: Dict[str, Any], model: Dict[str, Any], v: Optional[str] = None) -> Tuple[Dict[str, Any], List[str]]:
    node_logs = []
    # Create a semantic_model property for the model
    semantic_model_block = {
        "enabled": True,
    }
    if semantic_model.get("config"):
        semantic_model_block["config"] = semantic_model["config"]
    if semantic_model["name"] != model["name"]:
        semantic_model_block["name"] = model["name"]
    model["semantic_model"] = semantic_model_block

    # Propagate semantic model properties to the model
    if semantic_model.get("description"):
        if model.get("description"):
            model["description"] += model.get("description", "") + semantic_model["description"]
            node_logs.append(f"Appended semantic model 'description' to model 'description'.")
        else:
            model["description"] = semantic_model["description"]
            node_logs.append(f"Set model 'description' to semantic model 'description'.")
    
    if agg_time_dimension := semantic_model.get("defaults", {}).get("agg_time_dimension"):
        model["agg_time_dimension"] = agg_time_dimension
        node_logs.append(f"Set model 'agg_time_dimension' to semantic model 'agg_time_dimension'.")
    
    # TODO: support version for following methods + propagate version through
    # Propagate entities to model columns or derived_semantics
    node_logs.extend(merge_entities_with_model_columns(model, semantic_model.get("entities", [])))

    # Propagate dimensions to model columns or derived_semantics
    node_logs.extend(merge_dimensions_with_model_columns(model, semantic_model.get("dimensions", [])))

    # propagate measures to model metrics
    node_logs.extend(merge_measures_with_model_metrics(model, semantic_model.get("measures", [])))

    return model, node_logs

def merge_entities_with_model_columns(node: Dict[str, Any], entities: List[Dict[str, Any]], v: Optional[str] = None) -> List[str]:
    logs: List[str] = []
    node_columns = {column["name"]: column for column in node.get("columns", [])}
    versioned_node_columns = {}
    version_block = None
    if v:
        versions = node.get("versions", [])
        for version in versions:
            if version.get("v") == v:
                versioned_node_columns = {column["name"]: column for column in version.get("columns", [])}
                version_block = version
                break

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
        # Add entity to versioned column if it exists
        elif v and entity_col_name in versioned_node_columns:
            versioned_node_columns[entity_col_name]["entity"] = {
                "type": entity["type"]
            }
            if entity.get("name") != entity_col_name:
                versioned_node_columns[entity_col_name]["entity"]["name"] = entity["name"]
            logs.append(f"Added '{entity['type']}' entity to column '{entity_col_name}' for version '{v}'.")
        # If column doesn't exist, add a new one with new entity if no special characters in expr
        elif not any(char in entity_col_name for char in (" ", "|", "(")):
            # Add to existing columns lists
            if not v and node.get("columns"):
                node["columns"].append({
                    "name": entity_col_name,
                    "entity": {
                        "type": entity["type"]
                    }
                })
            elif version_block and version_block.get("columns"):
                version_block["columns"].append({
                    "name": entity_col_name,
                    "entity": {
                        "type": entity["type"]
                    }
                })
            else:
                if version_block:
                    version_block["columns"] = [{
                        "name": entity_col_name,
                        "entity": {
                            "type": entity["type"]
                        }
                    }]
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