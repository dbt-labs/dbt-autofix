import copy
from re import M
from typing import List, Tuple, Dict, Any, Optional, Union, Callable
from dbt_autofix.refactors.results import YMLRuleRefactorResult
from dbt_autofix.refactors.results import DbtDeprecationRefactor
from dbt_autofix.refactors.yml import DbtYAML, dict_to_yaml_str
from dbt_autofix.semantic_definitions import MeasureInput, SemanticDefinitions, ModelAccessHelpers
from rich.console import Console


def changeset_merge_simple_metrics_with_models(
    yml_str: str, semantic_definitions: SemanticDefinitions
) -> YMLRuleRefactorResult:
    return run_change_function_against_each_model(
        yml_str,
        semantic_definitions,
        combine_simple_metrics_with_their_input_measure,
        "merge_simple_metrics_with_model_metrics",
    )


def changeset_merge_complex_metrics_with_models(
    yml_str: str, semantic_definitions: SemanticDefinitions
) -> YMLRuleRefactorResult:
    return run_change_function_against_each_model(
        yml_str,
        semantic_definitions,
        merge_complex_metrics_with_model,
        "merge_complex_metrics_with_model_metrics",
    )


def run_change_function_against_each_model(
    yml_str: str,
    semantic_definitions: SemanticDefinitions,
    merge_fn: Callable,
    rule_name: str,
) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    for i, node in enumerate(yml_dict.get("models") or []):
        processed_node, node_refactored, node_refactor_logs = merge_fn(node, semantic_definitions)

        if node_refactored:
            refactored = True
            yml_dict["models"][i] = processed_node
            for log in node_refactor_logs:
                deprecation_refactors.append(DbtDeprecationRefactor(log=log, deprecation=None))

    return YMLRuleRefactorResult(
        rule_name=rule_name,
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
    )


def append_metric_to_model(
    model_node: Dict[str, Any],
    metric: Dict[str, Any],
) -> None:
    if "metrics" not in model_node:
        model_node["metrics"] = []
    model_node["metrics"].append(metric)


def combine_simple_metrics_with_their_input_measure(
    model_node: Dict[str, Any], semantic_definitions: SemanticDefinitions
) -> Tuple[Dict[str, Any], bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []

    semantic_model = semantic_definitions.get_semantic_model(model_node["name"]) or {}

    # for each simple metric in our semantic definitions, check if its measure is on this model
    # then flatten it by pulling the measure settings up into the metric.
    # Then, finally put the metric INTO the model that owned the measure.
    for metric_name, metric in semantic_definitions.initial_metrics.items():
        if metric["type"] != "simple":
            continue

        # Extract measure name from top-level simple metric
        measure_input = metric.get("type_params", {}).get("measure")
        measure_input = MeasureInput.parse_from_yaml(measure_input)
        if measure_input is None:
            # we've already fixed this one, so skip it.
            continue

        measure_name = measure_input.name
        fill_nulls_with = measure_input.fill_nulls_with
        join_to_timespine = measure_input.join_to_timespine
        measure = ModelAccessHelpers.maybe_get_measure_from_model(semantic_model, measure_name)
        if not measure:
            continue

        if measure.get("agg"):
            metric["agg"] = measure["agg"]
        # agg_params stuff
        if measure.get("percentile"):
            metric["percentile"] = measure["percentile"]
        if measure.get("use_discrete_percentile"):
            metric["use_discrete_percentile"] = measure["use_discrete_percentile"]
        if measure.get("use_approximate_percentile"):
            metric["use_approximate_percentile"] = measure["use_approximate_percentile"]

        if measure.get("agg_time_dimension"):
            metric["agg_time_dimension"] = measure["agg_time_dimension"]
        if measure.get("non_additive_dimension"):
            metric["non_additive_dimension"] = {}
            if measure["non_additive_dimension"].get("name"):
                metric["non_additive_dimension"]["name"] = measure["non_additive_dimension"]["name"]
            if measure["non_additive_dimension"].get("window_choice"):
                metric["non_additive_dimension"]["window_agg"] = measure["non_additive_dimension"]["window_choice"]
            if measure["non_additive_dimension"].get("window_groupings"):
                metric["non_additive_dimension"]["group_by"] = measure["non_additive_dimension"]["window_groupings"]

        if measure.get("expr"):
            metric["expr"] = measure["expr"]
        if fill_nulls_with:
            metric["fill_nulls_with"] = fill_nulls_with
        if join_to_timespine:
            metric["join_to_timespine"] = join_to_timespine

        # At this point, type_params should only include "measure", so we can just remove it wholely.
        metric.pop("type_params", {})

        if "metrics" not in model_node:
            model_node["metrics"] = []
        model_node["metrics"].append(metric)
        semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=measure_name)
        refactored = True
        refactor_logs.append(
            f"Folded input measure '{measure_name}' into simple metric '{metric_name}' and moved '{metric_name}' to model '{model_node['name']}'."
        )

    return model_node, refactored, refactor_logs


def _maybe_merge_cumulative_metric_with_model(
    metric: Dict[str, Any],
    model_node: Dict[str, Any],
    semantic_model: Dict[str, Any],
    semantic_definitions: SemanticDefinitions,
) -> Tuple[bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []
    metric_name = metric["name"]
    if metric_name in semantic_definitions.merged_metrics:
        # we've already merged this metric, so no need to do anything further!
        return refactored, refactor_logs

    measure_input = MeasureInput.parse_from_yaml(metric.get("type_params", {}).get("measure"))
    if measure_input is None:
        # This shouldn't happen; it seems like the measure was missing in the original yaml.
        return refactored, refactor_logs

    measure = ModelAccessHelpers.maybe_get_measure_from_model(semantic_model, measure_input.name)
    if measure is None:
        # The measure is not on THIS model, so we don't need to do anything here.
        return refactored, refactor_logs

    artificial_simple_metric, is_new_metric = get_or_create_metric_for_measure(
        measure=measure,
        fill_nulls_with=measure_input.fill_nulls_with,
        join_to_timespine=measure_input.join_to_timespine,
        is_hidden=True,
        semantic_definitions=semantic_definitions,
        dbt_model_node=model_node,
    )
    if not artificial_simple_metric:
        return refactored, refactor_logs
    
    if is_new_metric:
        refactor_logs.append(
            f"Added hidden simple metric '{artificial_simple_metric['name']}' to "
            f"model '{model_node['name']}' as input for cumulative metric '{metric_name}'.",
        )
    semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
    
    type_params = metric.pop("type_params", {})
    cumulative_type_params = type_params.pop("cumulative_type_params", None)

    if cumulative_type_params:
        if cumulative_type_params.get("window"):
            metric["window"] = cumulative_type_params.pop("window")
        if cumulative_type_params.get("grain_to_date"):
            metric["grain_to_date"] = cumulative_type_params.pop("grain_to_date")
        if cumulative_type_params.get("period_agg"):
            metric["period_agg"] = cumulative_type_params.pop("period_agg")
   
    metric["input_metric"] = measure_input.to_metric_input_yaml_obj(metric_name=artificial_simple_metric["name"])
    append_metric_to_model(model_node, metric)
    refactored = True
    refactor_logs.append(
        f"Added cumulative metric '{metric_name}' to model '{model_node['name']}'.",
    )

    return refactored, refactor_logs


def merge_complex_metrics_with_model(
    model_node: Dict[str, Any],
    semantic_definitions: SemanticDefinitions,
) -> Tuple[Dict[str, Any], bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []
    simple_metrics_on_model = {
        metric["name"]: metric for metric in model_node.get("metrics", []) if metric["type"] == "simple"
    }
    semantic_model = semantic_definitions.get_semantic_model(model_node["name"]) or {}
    if not semantic_model:
        # Nothing to work with here, so we should just skip this model.
        return model_node, refactored, refactor_logs

    # TODO: we should either loop until no changes are made (easier, but less efficient) or
    # do something along the lines of actually building and traversing a DAG of dependencies here.
    # Otherwise, we may miss a case where a metric's dependencies
    # are NOT already moved to the model so we skip it, but then the dependencies are moved afterward.

    # For each top-level metric, determine whether it can be merged with the model depending on its linked measures
    for metric_name, metric in semantic_definitions.initial_metrics.items():
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

                model_node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
                refactored = True
                refactor_logs.append(f"Added derived metric '{metric_name}' with to model '{model_node['name']}'.")
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

                model_node["metrics"].append(metric)
                semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
                refactored = True
                refactor_logs.append(f"Added ratio metric '{metric_name}' to model '{model_node['name']}'.")

        elif metric["type"] == "cumulative":
            metric_refactored, metric_refactor_logs = _maybe_merge_cumulative_metric_with_model(
                metric, model_node, semantic_model, semantic_definitions
            )
            refactored = refactored or metric_refactored
            refactor_logs.extend(metric_refactor_logs)

        elif metric["type"] == "conversion":
            base_measure = metric.get("type_params", {}).get("conversion_type_params", {}).get("base_measure")
            base_measure_fill_nulls_with = (
                base_measure.get("fill_nulls_with") if isinstance(base_measure, dict) else None
            )
            base_measure_join_to_timespine = (
                base_measure.get("join_to_timespine") if isinstance(base_measure, dict) else None
            )
            raw_base_measure_name, base_measure_name = _get_name_from_measure_input_deprecated(base_measure)

            conversion_measure = (
                metric.get("type_params", {}).get("conversion_type_params", {}).get("conversion_measure")
            )
            conversion_measure_fill_nulls_with = (
                conversion_measure.get("fill_nulls_with") if isinstance(conversion_measure, dict) else None
            )
            conversion_measure_join_to_timespine = (
                conversion_measure.get("join_to_timespine") if isinstance(conversion_measure, dict) else None
            )
            raw_conversion_measure_name, conversion_measure_name = _get_name_from_measure_input_deprecated(
                conversion_measure
            )

            add_conversion_metric_to_model = False
            add_hidden_base_metric_to_model = False
            add_hidden_conversion_metric_to_model = False
            if base_measure_name in simple_metrics_on_model and conversion_measure_name in simple_metrics_on_model:
                add_conversion_metric_to_model = True
            # Both base and conversion measures need simple metrics created
            elif (
                raw_base_measure_name in simple_metrics_on_model
                and raw_conversion_measure_name in simple_metrics_on_model
            ):
                add_conversion_metric_to_model = True
                add_hidden_base_metric_to_model = True
                add_hidden_conversion_metric_to_model = True
            # Only conversion measure needs a simple metric created
            elif (
                raw_conversion_measure_name in simple_metrics_on_model and base_measure_name in simple_metrics_on_model
            ):
                add_conversion_metric_to_model = True
                add_hidden_conversion_metric_to_model = True
            # Only base measure needs a simple metric created
            elif (
                raw_base_measure_name in simple_metrics_on_model and conversion_measure_name in simple_metrics_on_model
            ):
                add_conversion_metric_to_model = True
                add_hidden_base_metric_to_model = True

            if add_hidden_base_metric_to_model:
                new_simple_metric = _create_hidden_simple_metric_from_deprecated(
                    simple_metrics_on_model[raw_base_measure_name],
                    base_measure_name,
                    base_measure_fill_nulls_with,
                    base_measure_join_to_timespine,
                )
                model_node["metrics"].append(new_simple_metric)
                refactored = True
                refactor_logs.append(
                    f"Added hidden simple metric '{base_measure_name}' to model '{model_node['name']}'."
                )

            if add_hidden_conversion_metric_to_model:
                new_simple_metric = _create_hidden_simple_metric_from_deprecated(
                    simple_metrics_on_model[raw_conversion_measure_name],
                    conversion_measure_name,
                    conversion_measure_fill_nulls_with,
                    conversion_measure_join_to_timespine,
                )
                model_node["metrics"].append(new_simple_metric)
                refactored = True
                refactor_logs.append(
                    f"Added hidden simple metric '{conversion_measure_name}' to model '{model_node['name']}'."
                )

            if add_conversion_metric_to_model:
                model_node["metrics"].append(
                    migrate_conversion_metric(metric, base_measure_name, conversion_measure_name)
                )
                semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
                refactored = True
                refactor_logs.append(f"Added conversion metric '{metric_name}' to model '{model_node['name']}'.")

    return model_node, refactored, refactor_logs


def make_artificial_metric_name(
    measure_name: str,
    fill_nulls_with: Optional[str],
    join_to_timespine: Optional[bool],
    semantic_definitions: SemanticDefinitions,
) -> str:
    base_name = measure_name
    if fill_nulls_with is not None and fill_nulls_with != "":
        base_name += f"_fill_nulls_with_{fill_nulls_with}"
    if join_to_timespine:
        base_name += "_join_to_timespine"

    # increment to avoid duplication if another metric by this name was created by the
    # original (probably human) yaml authors.
    final_name = base_name
    i = 1
    original_metric_names = semantic_definitions.initial_metrics.keys()
    # if the name existed originally or somehow we've already added it, keep incrementing
    # to be safe.
    while final_name in original_metric_names or semantic_definitions.artificial_metric_name_exists(final_name):
        final_name = f"{base_name}_{i}"
        i += 1

    return final_name


def get_or_create_metric_for_measure(
    measure: Dict[str, Any],
    fill_nulls_with: Optional[str],
    join_to_timespine: Optional[bool],
    is_hidden: bool,
    semantic_definitions: SemanticDefinitions,
    dbt_model_node: Dict[str, Any],
) -> Tuple[Dict[str, Any], bool]:
    """Returns tuple(metric, is_new_metric)."""
    measure_name = measure["name"]

    # if we already built this one before, reuse that one.
    if artificial_metric := semantic_definitions.get_artificial_metric(
        measure_name=measure_name,
        fill_nulls_with=fill_nulls_with,
        join_to_timespine=join_to_timespine,
    ):
        return artificial_metric, False

    artificial_metric_name = make_artificial_metric_name(
        measure_name=measure_name,
        fill_nulls_with=fill_nulls_with,
        join_to_timespine=join_to_timespine,
        semantic_definitions=semantic_definitions,
    )
    # deep copy so we can confidently make other copies of this metric without interfering with the original.
    artificial_metric = copy.deepcopy(measure)
    artificial_metric["name"] = artificial_metric_name
    artificial_metric["type"] = "simple"
    if is_hidden:
        artificial_metric["hidden"] = True
    artificial_metric.pop("create_metric", {})
    # The following is copied from earlier versions of this script and might bear further testing.
    # Renamed non_additive_dimension keys
    if artificial_metric.get("non_additive_dimension"):
        # window_choice -> window_agg
        window_choice = artificial_metric["non_additive_dimension"].pop("window_choice", None)
        if window_choice:
            artificial_metric["non_additive_dimension"]["window_agg"] = window_choice
        # window_groupings -> group_by
        window_groupings = artificial_metric["non_additive_dimension"].pop("window_groupings", None)
        if window_groupings:
            artificial_metric["non_additive_dimension"]["group_by"] = window_groupings

    semantic_definitions.record_artificial_metric(
        measure_name=measure_name,
        fill_nulls_with=fill_nulls_with,
        join_to_timespine=join_to_timespine,
        metric=artificial_metric,
    )
    append_metric_to_model(dbt_model_node, artificial_metric)
    return artificial_metric, True


def add_metric_for_measures_in_model(
    model_node: Dict[str, Any],
    semantic_definitions: SemanticDefinitions,
) -> Tuple[Dict[str, Any], bool, List[str]]:
    """Add metrics for the measures in a semantic model."""
    refactored = False
    refactor_logs: List[str] = []

    semantic_model = semantic_definitions.get_semantic_model(model_node["name"]) or {}

    def create_simple_metric_from_measure(measure: Dict[str, Any], is_hidden: bool) -> Tuple[Dict[str, Any], bool]:
        # since these are measures with no measure input wrapper, there are no available
        # values for fill_nulls_with and join_to_timespine.
        return get_or_create_metric_for_measure(
            measure=measure,
            fill_nulls_with=None,
            join_to_timespine=None,
            is_hidden=is_hidden,
            semantic_definitions=semantic_definitions,
            dbt_model_node=model_node,
        )

    for measure in ModelAccessHelpers.get_measures_from_model(semantic_model):
        measure_name = measure["name"]
        metric = None
        is_new_metric = False
        # if there was a metric with this name to begin with, skip this step
        #   because we already ignored create_metric=True directives in that case
        #   in dbt-semantic-interfaces and metricflow.
        if measure_name in semantic_definitions.initial_metrics:
            continue
        # if we've already created an artificial metric for this measure, don't do it again!
        elif semantic_definitions.artificial_metric_name_exists(measure_name):
            continue
        # elif create_metric = True, if it's not already in our list of artificial metrics, create it with hidden = False
        elif measure.get("create_metric", False):
            metric, is_new_metric = create_simple_metric_from_measure(measure, is_hidden=False)
        # Optionally, we can add metrics for measures that are never consumed...
        else:
            # since we're just preserving measures here, don't make a new metric if
            # it was merged into something already.
            if semantic_definitions.measure_is_merged(measure_name):
                continue
            # Let's convert this currently unused measure as it's probably a human's WIP
            metric, is_new_metric = create_simple_metric_from_measure(measure, is_hidden=True)

        if is_new_metric:
            refactored = True
            refactor_logs.append(f"Added simple metric '{metric.get('name')}' to model '{model_node['name']}'.")

    return model_node, refactored, refactor_logs


def changeset_add_metrics_for_measures(
    yml_str: str,
    semantic_definitions: SemanticDefinitions,
) -> YMLRuleRefactorResult:
    return run_change_function_against_each_model(
        yml_str,
        semantic_definitions,
        add_metric_for_measures_in_model,
        "add_new_metrics_for_measures_to_model",
    )


def _get_name_from_measure_input_deprecated(measure: Union[str, Dict[str, Any]]) -> Tuple[str, str]:
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


def _create_hidden_simple_metric_from_deprecated(
    other_simple_metric: Dict[str, Any],
    name: str,
    fill_nulls_with: Optional[str] = None,
    join_to_timespine: Optional[bool] = None,
) -> Dict[str, Any]:
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


def migrate_conversion_metric(
    metric: Dict[str, Any], base_measure_name: str, conversion_measure_name: str
) -> Dict[str, Any]:
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


def changeset_merge_semantic_models_with_models(
    yml_str: str, semantic_definitions: SemanticDefinitions
) -> YMLRuleRefactorResult:
    refactored = False
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    yml_dict = DbtYAML().load(yml_str) or {}

    # Merge semantic models with existing models in yml
    for i, node in enumerate(yml_dict.get("models") or []):
        processed_node, node_refactored, node_refactor_logs = merge_semantic_models_with_model(
            node, semantic_definitions
        )

        if node_refactored:
            refactored = True
            yml_dict["models"][i] = processed_node
            for log in node_refactor_logs:
                deprecation_refactors.append(DbtDeprecationRefactor(log=log, deprecation=None))

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

            processed_new_model_node, new_model_node_refactored, new_model_node_refactor_logs = (
                merge_semantic_models_with_model(new_model_node, semantic_definitions)
            )
            if new_model_node_refactored:
                refactored = True
                yml_dict["models"].append(processed_new_model_node)
                for log in new_model_node_refactor_logs:
                    deprecation_refactors.append(DbtDeprecationRefactor(log=log, deprecation=None))

    return YMLRuleRefactorResult(
        rule_name="restructure_owner_properties",
        refactored=refactored,
        refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
        original_yaml=yml_str,
        deprecation_refactors=deprecation_refactors,
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

            refactored = True
            refactor_log = f"Model '{node['name']}' - Merged with semantic model '{semantic_model['name']}'."
            semantic_definitions.mark_semantic_model_as_merged(semantic_model["name"], node["name"])
            for log in node_logs:
                refactor_log += f"\n\t* {log}"
            refactor_logs.append(refactor_log)

    return node, refactored, refactor_logs


def merge_entities_with_model_columns(node: Dict[str, Any], entities: List[Dict[str, Any]]) -> List[str]:
    logs: List[str] = []
    node_columns = {column["name"]: column for column in node.get("columns", [])}

    for entity in entities:
        entity_col_name = entity.get("expr") or entity["name"]

        # Add entity to column if column already exists
        if entity_col_name in node_columns:
            node_columns[entity_col_name]["entity"] = {"type": entity["type"]}
            if entity.get("name") != entity_col_name:
                node_columns[entity_col_name]["entity"]["name"] = entity["name"]
            logs.append(f"Added '{entity['type']}' entity to column '{entity_col_name}'.")
        # If column doesn't exist, add a new one with new entity if no special characters in expr
        elif not any(char in entity_col_name for char in (" ", "|", "(")):
            if node.get("columns"):
                node["columns"].append({"name": entity_col_name, "entity": {"type": entity["type"]}})
            else:
                node["columns"] = [{"name": entity_col_name, "entity": {"type": entity["type"]}}]
            logs.append(f"Added new column '{entity_col_name}' with '{entity['type']}' entity.")
        # Create entity as derived semantic entity
        else:
            if "derived_semantics" not in node:
                node["derived_semantics"] = {"entities": []}

            if "entities" not in node["derived_semantics"]:
                node["derived_semantics"]["entities"] = []

            node["derived_semantics"]["entities"].append(
                {
                    "name": entity_col_name,
                    "type": entity["type"],
                }
            )
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
            node_columns[dimension_col_name]["dimension"] = {"type": dimension["type"]}
            # Add time granularity to top-level column if it was defined on the dimension
            if dimension_time_granularity:
                node_columns[dimension_col_name]["granularity"] = dimension_time_granularity
            logs.append(f"Added '{dimension['type']}' dimension to column '{dimension_col_name}'.")
        # If column doesn't exist, add a new one with new dimension if no special characters in expr
        elif not any(char in dimension_col_name for char in (" ", "|", "(")):
            if node.get("columns"):
                node["columns"].append({"name": dimension_col_name, "dimension": {"type": dimension["type"]}})
            else:
                node["columns"] = [{"name": dimension_col_name, "dimension": {"type": dimension["type"]}}]
            # Add time granularity to top-level column if it was defined on the dimension
            if dimension_time_granularity:
                node["columns"][-1]["granularity"] = dimension_time_granularity
            logs.append(f"Added new column '{dimension_col_name}' with '{dimension['type']}' dimension.")
        # Create entity as derived semantic entity
        else:
            if "derived_semantics" not in node:
                node["derived_semantics"] = {"entities": []}
            if "dimensions" not in node["derived_semantics"]:
                node["derived_semantics"]["dimensions"] = []

            node["derived_semantics"]["dimensions"].append(
                {
                    "name": dimension_col_name,
                    "type": dimension["type"],
                }
            )
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
        metric = {"name": metric_name, "type": "simple", "label": measure.get("label") or metric_name}
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
            logs.append(
                f"Updated existing metric '{metric_name}' with measure '{metric_name}' from semantic model '{node['name']}'."
            )
        else:
            if "metrics" not in node:
                node["metrics"] = []
            node["metrics"].append(metric)
            logs.append(
                f"Added new simple metric '{metric_name}' from measure '{metric_name}' on semantic model '{node['name']}'."
            )

    return logs


def changeset_delete_top_level_semantic_models(
    yml_str: str, semantic_definitions: SemanticDefinitions
) -> YMLRuleRefactorResult:
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
                    log=f"Deleted top-level semantic model '{semantic_model['name']}'.", deprecation=None
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
        deprecation_refactors=deprecation_refactors,
    )


def changeset_migrate_or_delete_top_level_metrics(
    yml_str: str, semantic_definitions: SemanticDefinitions
) -> YMLRuleRefactorResult:
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
                DbtDeprecationRefactor(log=f"Deleted top-level metric '{metric['name']}'.", deprecation=None)
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
                    deprecation=None,
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
        deprecation_refactors=deprecation_refactors,
    )
