import copy
from typing import Any, Dict, List, Optional, Tuple, Union

from ruamel.yaml.comments import CommentedMap, CommentedSeq

from dbt_autofix.deprecations import ChangeType
from dbt_autofix.refactors.node import (
    Node,
    append_node,
    delete_top_level_key,
    extract_deep_trailing_above_comment,
    extract_node,
    insert_at_deep_trailing,
)
from dbt_autofix.refactors.results import (
    DbtDeprecationRefactor,
    RefactorEntry,
    YMLContent,
    YMLRefactorConfig,
    YMLRuleRefactorResult,
    location_of_key,
    location_of_node,
)
from dbt_autofix.refactors.yml import dict_to_yaml_str, get_dict, get_list, load_yaml
from dbt_autofix.semantic_definitions import MeasureInput, ModelAccessHelpers, SemanticDefinitions


def changeset_merge_simple_metrics_with_models(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _MergeSimpleMetricsImpl(content, config).execute()


def changeset_merge_complex_metrics_with_models(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _MergeComplexMetricsImpl(content, config).execute()


def changeset_add_metrics_for_measures(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _AddMetricsForMeasuresImpl(content, config).execute()


def changeset_merge_semantic_models_with_models(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _MergeSemanticModelsImpl(content, config).execute()


def changeset_delete_top_level_semantic_models(content: YMLContent, config: YMLRefactorConfig) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _DeleteTopLevelSemanticModelsImpl(content, config).execute()


def changeset_migrate_metric_tags_field_to_config(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _MigrateMetricTagsImpl(content, config).execute()


def changeset_migrate_or_delete_top_level_metrics(
    content: YMLContent, config: YMLRefactorConfig
) -> YMLRuleRefactorResult:
    assert config.semantic_definitions is not None
    return _MigrateOrDeleteTopLevelMetricsImpl(content, config).execute()


# ---------------------------------------------------------------------------
# Module-level helpers (pure transformations, no entry accumulation)
# ---------------------------------------------------------------------------


def append_metric_to_model(
    model_node: CommentedMap,
    metric: CommentedMap,
    semantic_definitions: Optional[SemanticDefinitions] = None,
    seq_comment: Optional[list] = None,
) -> None:
    if "metrics" not in model_node:
        model_node["metrics"] = CommentedSeq()
    if seq_comment is None and semantic_definitions is not None:
        seq_comment = semantic_definitions.initial_metric_seq_comments.get(metric["name"])
    append_node(model_node["metrics"], Node(value=metric, original_location=None, comments=seq_comment))


def get_metric_input_dict(metric: Union[str, CommentedMap]) -> CommentedMap:
    if isinstance(metric, str):
        return CommentedMap({"name": metric})
    return metric


def change_metrics_to_input_metrics(metric: CommentedMap) -> None:
    """Currently only used for derived metrics."""
    if "metrics" in metric:
        metric["input_metrics"] = [get_metric_input_dict(input_metric) for input_metric in metric.pop("metrics", [])]


def _get_metric_from_model_or_top_level(
    metric_name: str,
    model_node: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Optional[CommentedMap]:
    """Returns the metric from the model if possible, else falls back to initial top-level metrics."""
    metric_from_model = next(
        (metric for metric in get_list(model_node, "metrics") if metric["name"] == metric_name), None
    )
    if metric_from_model:
        return metric_from_model
    return semantic_definitions.initial_metrics.get(metric_name)


def _get_metric_name_from_metric_input(metric_input: Union[str, CommentedMap]) -> str:
    if isinstance(metric_input, str):
        return metric_input
    return metric_input["name"]


def is_useful_fill_nulls_with_value(fill_nulls_with: Optional[str]) -> bool:
    return fill_nulls_with is not None and fill_nulls_with != ""


def make_artificial_metric_name(
    measure_name: str,
    fill_nulls_with: Optional[str],
    join_to_timespine: Optional[bool],
    semantic_definitions: SemanticDefinitions,
) -> str:
    base_name = measure_name
    # the 'is not None' check here is duplicated inside the called function, but it's here for typechecking
    if is_useful_fill_nulls_with_value(fill_nulls_with) and fill_nulls_with is not None:
        try:
            val = int(fill_nulls_with)
            fill_nulls_with_str = f"negative_{abs(val)}" if val < 0 else str(val)
        except ValueError:
            fill_nulls_with_str = str(fill_nulls_with)

        base_name += f"_fill_nulls_with_{fill_nulls_with_str}"
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
    measure: CommentedMap,
    fill_nulls_with: Optional[str],
    join_to_timespine: Optional[bool],
    is_hidden: bool,
    semantic_definitions: SemanticDefinitions,
    dbt_model_node: CommentedMap,
) -> Tuple[CommentedMap, bool]:
    """Returns tuple(metric, is_new_metric)."""
    measure_name = measure["name"]

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
    artificial_metric = copy.deepcopy(measure)
    artificial_metric["name"] = artificial_metric_name
    artificial_metric["type"] = "simple"
    if is_hidden:
        artificial_metric["hidden"] = True
    artificial_metric.pop("create_metric", {})
    if artificial_metric.get("non_additive_dimension"):
        window_choice = artificial_metric["non_additive_dimension"].pop("window_choice", None)
        if window_choice:
            artificial_metric["non_additive_dimension"]["window_agg"] = window_choice
        window_groupings = artificial_metric["non_additive_dimension"].pop("window_groupings", None)
        if window_groupings:
            artificial_metric["non_additive_dimension"]["group_by"] = window_groupings

    if is_useful_fill_nulls_with_value(fill_nulls_with):
        artificial_metric["fill_nulls_with"] = fill_nulls_with
    if join_to_timespine is not None:
        artificial_metric["join_to_timespine"] = join_to_timespine

    if agg_params := artificial_metric.pop("agg_params", None):
        if agg_percentile := agg_params.get("percentile"):
            artificial_metric["percentile"] = agg_percentile
        use_discrete_percentile = agg_params.get("use_discrete_percentile")
        use_approximate_percentile = agg_params.get("use_approximate_percentile")
        if use_discrete_percentile and use_approximate_percentile:
            raise ValueError(
                f"Both use_discrete_percentile and use_approximate_percentile cannot be true for measure {measure_name}"
            )
        if use_discrete_percentile:
            artificial_metric["percentile_type"] = "discrete"
        if use_approximate_percentile:
            artificial_metric["percentile_type"] = "approximate"

    semantic_definitions.record_artificial_metric(
        measure_name=measure_name,
        fill_nulls_with=fill_nulls_with,
        join_to_timespine=join_to_timespine,
        metric=artificial_metric,
    )

    measure_seq_comment = copy.deepcopy(semantic_definitions.initial_measure_seq_comments.get(measure_name))
    append_metric_to_model(dbt_model_node, artificial_metric, semantic_definitions, seq_comment=measure_seq_comment)
    return artificial_metric, True


def merge_entities_with_model_columns(node: CommentedMap, entities: List[CommentedMap]) -> List[str]:
    r"""Merges entities from a semantic model into the model's columns.

    This function assumes you've already limited the entity list to those that could be
    on this model, based on matching the model and semantic model.
    """
    logs: List[str] = []
    node_columns = {column["name"]: column for column in get_list(node, "columns")}

    for entity in entities:
        entity_name = entity["name"]
        entity_expr = entity.get("expr")
        entity_col_name = entity_expr or entity_name

        def make_entity_dict() -> Dict[str, Any]:
            entity_dict = {"type": entity["type"]}
            if entity_name != entity_col_name:
                entity_dict["name"] = entity_name
            return entity_dict

        if entity_col_name in node_columns:
            node_columns[entity_col_name]["entity"] = make_entity_dict()
            logs.append(f"Added '{entity['type']}' entity to column '{entity_col_name}'.")
        elif not any(char in entity_col_name for char in (" ", "|", "(")):
            if not node.get("columns"):
                node["columns"] = []
            node["columns"].append(
                {
                    "name": entity_col_name,
                    "entity": make_entity_dict(),
                }
            )
            logs.append(f"Added new column '{entity_col_name}' with '{entity['type']}' entity.")
        else:
            if "derived_semantics" not in node:
                node["derived_semantics"] = {"entities": []}

            if "entities" not in node["derived_semantics"]:
                node["derived_semantics"]["entities"] = []

            new_entity = {
                "name": entity_name,
                "type": entity["type"],
            }
            if entity_expr:
                new_entity["expr"] = entity_expr
            node["derived_semantics"]["entities"].append(new_entity)
            logs.append(f"Added 'derived_semantics' to model with '{entity['type']}' entity.")

    return logs


def merge_dimensions_with_model_columns(model_node: CommentedMap, dimensions: List[CommentedMap]) -> List[str]:
    logs: List[str] = []
    model_node_columns = {column["name"]: column for column in get_list(model_node, "columns")}

    for dimension in dimensions:
        dimension_col_name = dimension.get("expr") or dimension["name"]
        dim_name = dimension["name"]
        dim_expr = dimension.get("expr")

        def is_valid_name(name: str) -> bool:
            return not any(char in name for char in (" ", "|", "("))

        dimension_time_granularity = get_dict(dimension, "type_params").get("time_granularity")

        def get_mergeable_dimension_fields() -> Dict[str, Any]:
            base_dim_dict = {
                "type": dimension["type"],
            }
            if dimension_col_name != dim_name:
                base_dim_dict["name"] = dim_name
            column_fields = {
                "dimension": base_dim_dict,
            }
            if dimension_time_granularity:
                column_fields["granularity"] = dimension_time_granularity
            return column_fields

        if dimension_col_name in model_node_columns:
            model_node_columns[dimension_col_name].update(get_mergeable_dimension_fields())
            logs.append(f"Added '{dimension['type']}' dimension to column '{dimension_col_name}'.")
        elif is_valid_name(dimension_col_name):
            if not model_node.get("columns", False):
                model_node["columns"] = []
            model_node["columns"].append(
                {
                    "name": dimension_col_name,
                    **get_mergeable_dimension_fields(),
                }
            )
            logs.append(f"Added new column '{dimension_col_name}' with '{dimension['type']}' dimension.")
        else:
            if "derived_semantics" not in model_node:
                model_node["derived_semantics"] = {"entities": []}
            if "dimensions" not in model_node["derived_semantics"]:
                model_node["derived_semantics"]["dimensions"] = []

            new_dim = {
                "name": dim_name,
                "type": dimension["type"],
            }
            if dimension_time_granularity:
                new_dim["granularity"] = dimension_time_granularity
            if dim_expr:
                new_dim["expr"] = dim_expr
            model_node["derived_semantics"]["dimensions"].append(new_dim)

            logs.append(f"Added 'derived_semantics' to model with '{dimension['type']}' entity.")

    return logs


# ---------------------------------------------------------------------------
# Merge-function helpers (return (node, refactored, logs) — called by _Impl methods)
# ---------------------------------------------------------------------------


def combine_simple_metrics_with_their_input_measure(
    model_node: CommentedMap, semantic_definitions: SemanticDefinitions
) -> Tuple[CommentedMap, bool, List[Tuple[str, Optional[str]]]]:
    refactored = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []

    semantic_model = semantic_definitions.get_semantic_model(model_node["name"])

    for metric_name, metric in semantic_definitions.initial_metrics.items():
        if metric["type"] != "simple":
            continue

        measure_input = MeasureInput.parse_from_yaml(
            get_dict(metric, "type_params").get("measure"),
        )
        if measure_input is None:
            continue

        measure_name = measure_input.name
        fill_nulls_with = measure_input.fill_nulls_with
        join_to_timespine = measure_input.join_to_timespine
        measure = ModelAccessHelpers.maybe_get_measure_from_model(semantic_model, measure_name)
        if not measure:
            continue

        # Strip any stale above-comment from the metric before mutating it (adding agg/expr
        # would shift filter away from last-key position, making the token unreachable).
        extract_deep_trailing_above_comment(metric)

        if measure.get("agg"):
            metric["agg"] = measure["agg"]
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
        if is_useful_fill_nulls_with_value(fill_nulls_with):
            metric["fill_nulls_with"] = fill_nulls_with
        if join_to_timespine:
            metric["join_to_timespine"] = join_to_timespine

        if measure_input.filter:
            metric_filter = metric.get("filter", None)
            if metric_filter:
                metric_filter = f"({metric_filter}) AND ({measure_input.filter})"
            else:
                metric_filter = measure_input.filter
            metric["filter"] = metric_filter

        # At this point, type_params should only include "measure", so we can just remove it wholely.
        delete_top_level_key(metric, "type_params")

        append_metric_to_model(model_node, metric, semantic_definitions)
        semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=measure_name)
        refactored = True
        refactor_logs.append(
            (
                f"Folded input measure '{measure_name}' into simple metric '{metric_name}' and moved '{metric_name}' to model '{model_node['name']}'.",
                metric_name,
            )
        )

    return model_node, refactored, refactor_logs


def _maybe_merge_cumulative_metric_with_model(
    metric: CommentedMap,
    model_node: CommentedMap,
    semantic_model: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Tuple[bool, List[Tuple[str, Optional[str]]], bool]:
    refactored = False
    moved_to_model = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []
    metric_name = metric["name"]
    if metric_name in semantic_definitions.merged_metrics:
        return refactored, refactor_logs, moved_to_model

    measure_input = MeasureInput.parse_from_yaml(get_dict(metric, "type_params").get("measure"))
    if measure_input is None:
        return refactored, refactor_logs, moved_to_model

    measure = ModelAccessHelpers.maybe_get_measure_from_model(semantic_model, measure_input.name)
    if measure is None:
        return refactored, refactor_logs, moved_to_model

    artificial_simple_metric, is_new_metric = get_or_create_metric_for_measure(
        measure=measure,
        fill_nulls_with=measure_input.fill_nulls_with,
        join_to_timespine=measure_input.join_to_timespine,
        is_hidden=True,
        semantic_definitions=semantic_definitions,
        dbt_model_node=model_node,
    )
    if not artificial_simple_metric:
        return refactored, refactor_logs, moved_to_model

    if is_new_metric:
        refactor_logs.append(
            (
                f"Added hidden simple metric '{artificial_simple_metric['name']}' to "
                f"model '{model_node['name']}' as input for cumulative metric '{metric_name}'.",
                artificial_simple_metric["name"],
            )
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
    append_metric_to_model(model_node, metric, semantic_definitions)
    refactored = True
    refactor_logs.append(
        (
            f"Added cumulative metric '{metric_name}' to model '{model_node['name']}'.",
            metric_name,
        )
    )

    moved_to_model = refactored
    return refactored, refactor_logs, moved_to_model


def _maybe_merge_conversion_metric_with_model(
    metric: CommentedMap,
    model_node: CommentedMap,
    semantic_model: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Tuple[bool, List[Tuple[str, Optional[str]]], bool]:
    refactored = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []
    base_metric_in_model = False
    conversion_metric_in_model = False
    moved_to_model = False

    metric_name = metric["name"]
    if metric_name in semantic_definitions.merged_metrics:
        return refactored, refactor_logs, moved_to_model
    type_params = get_dict(metric, "type_params")
    conversion_type_params = get_dict(type_params, "conversion_type_params")
    base_measure_input = MeasureInput.parse_from_yaml(conversion_type_params.get("base_measure", None))
    if base_measure_input and (
        base_measure := ModelAccessHelpers.maybe_get_measure_from_model(semantic_model, base_measure_input.name)
    ):
        artificial_base_metric, is_new_base_metric = get_or_create_metric_for_measure(
            measure=base_measure,
            fill_nulls_with=base_measure_input.fill_nulls_with,
            join_to_timespine=base_measure_input.join_to_timespine,
            is_hidden=True,
            semantic_definitions=semantic_definitions,
            dbt_model_node=model_node,
        )
        if is_new_base_metric:
            refactor_logs.append(
                (
                    f"Added hidden simple metric '{artificial_base_metric['name']}' to "
                    f"model '{model_node['name']}' as base_metric input for conversion metric '{metric_name}'.",
                    artificial_base_metric["name"],
                )
            )
        metric["base_metric"] = base_measure_input.to_metric_input_yaml_obj(
            metric_name=artificial_base_metric["name"],
        )
        refactored = True
        base_metric_in_model = True
        conversion_type_params.pop("base_measure", None)

    conversion_measure_input = MeasureInput.parse_from_yaml(conversion_type_params.get("conversion_measure", None))
    if conversion_measure_input and (
        conversion_measure := ModelAccessHelpers.maybe_get_measure_from_model(
            semantic_model, conversion_measure_input.name
        )
    ):
        artificial_conversion_metric, is_new_conversion_metric = get_or_create_metric_for_measure(
            measure=conversion_measure,
            fill_nulls_with=conversion_measure_input.fill_nulls_with,
            join_to_timespine=conversion_measure_input.join_to_timespine,
            is_hidden=True,
            semantic_definitions=semantic_definitions,
            dbt_model_node=model_node,
        )
        if is_new_conversion_metric:
            refactor_logs.append(
                (
                    f"Added hidden simple metric '{artificial_conversion_metric['name']}' to "
                    f"model '{model_node['name']}' as conversion_metric input for conversion metric '{metric_name}'.",
                    artificial_conversion_metric["name"],
                )
            )
        metric["conversion_metric"] = conversion_measure_input.to_metric_input_yaml_obj(
            metric_name=artificial_conversion_metric["name"],
        )
        refactored = True
        conversion_metric_in_model = True
        conversion_type_params.pop("conversion_measure", None)

    if base_metric_in_model and conversion_metric_in_model:
        append_metric_to_model(model_node, metric, semantic_definitions)
        semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
        refactor_logs.append((f"Added conversion metric '{metric_name}' to model '{model_node['name']}'.", metric_name))
        refactored = True
        metric.update(conversion_type_params)
        type_params.pop("conversion_type_params", None)
        metric.update(type_params)
        metric.pop("type_params", None)
        moved_to_model = True

    return refactored, refactor_logs, moved_to_model


def try_to_merge_complex_metric_with_model_recursive(
    metric: CommentedMap,
    model_node: CommentedMap,
    semantic_model: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Tuple[bool, List[Tuple[str, Optional[str]]], bool]:
    refactored = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []

    metric_name = metric["name"]

    if metric["type"] == "simple" or metric_name in semantic_definitions.merged_metrics:
        is_on_model = metric_name in [metric["name"] for metric in get_list(model_node, "metrics")]
        return refactored, refactor_logs, is_on_model

    if metric["type"] == "cumulative":
        metric_refactored, metric_refactor_logs, moved_to_model = _maybe_merge_cumulative_metric_with_model(
            metric,
            model_node,
            semantic_model,
            semantic_definitions,
        )
        refactored = refactored or metric_refactored
        refactor_logs.extend(metric_refactor_logs)
        return refactored, refactor_logs, moved_to_model

    if metric["type"] == "conversion":
        metric_refactored, metric_refactor_logs, moved_to_model = _maybe_merge_conversion_metric_with_model(
            metric,
            model_node,
            semantic_model,
            semantic_definitions,
        )
        refactored = refactored or metric_refactored
        refactor_logs.extend(metric_refactor_logs)
        return refactored, refactor_logs, moved_to_model

    if metric["type"] == "derived":
        input_metric_names: List[str] = []
        for input_metric in get_list(get_dict(metric, "type_params"), "metrics"):
            input_metric_names.append(_get_metric_name_from_metric_input(input_metric))

        moved_to_model = True
        for input_metric_name in input_metric_names:
            input_metric = _get_metric_from_model_or_top_level(input_metric_name, model_node, semantic_definitions)
            if not input_metric:
                moved_to_model = False
                break

            sub_refactored, sub_refactor_logs, sub_moved_to_model = try_to_merge_complex_metric_with_model_recursive(
                input_metric,
                model_node,
                semantic_model,
                semantic_definitions,
            )
            refactored = refactored or sub_refactored
            refactor_logs.extend(sub_refactor_logs)
            if not sub_moved_to_model:
                moved_to_model = False
                break
        if moved_to_model:
            type_params = metric.pop("type_params", {})
            metric.update(type_params)
            change_metrics_to_input_metrics(metric)

            append_metric_to_model(model_node, metric, semantic_definitions)
            semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
            refactored = True
            refactor_logs.append(
                (f"Added derived metric '{metric_name}' with to model '{model_node['name']}'.", metric_name)
            )

        return refactored, refactor_logs, moved_to_model

    if metric["type"] == "ratio":
        moved_to_model = False
        numerator_name = _get_metric_name_from_metric_input(get_dict(metric, "type_params").get("numerator"))
        denominator_name = _get_metric_name_from_metric_input(get_dict(metric, "type_params").get("denominator"))
        numerator_metric = _get_metric_from_model_or_top_level(numerator_name, model_node, semantic_definitions)
        denominator_metric = _get_metric_from_model_or_top_level(denominator_name, model_node, semantic_definitions)
        if not numerator_metric or not denominator_metric:
            return refactored, refactor_logs, False
        numerator_refactored, numerator_refactor_logs, numerator_moved_to_model = (
            try_to_merge_complex_metric_with_model_recursive(
                numerator_metric,
                model_node,
                semantic_model,
                semantic_definitions,
            )
        )
        denominator_refactored, denominator_refactor_logs, denominator_moved_to_model = (
            try_to_merge_complex_metric_with_model_recursive(
                denominator_metric,
                model_node,
                semantic_model,
                semantic_definitions,
            )
        )
        refactored = refactored or numerator_refactored or denominator_refactored
        refactor_logs.extend(numerator_refactor_logs)
        refactor_logs.extend(denominator_refactor_logs)
        if numerator_moved_to_model and denominator_moved_to_model:
            type_params = metric.pop("type_params", {})
            metric.update(type_params)
            append_metric_to_model(model_node, metric, semantic_definitions)
            semantic_definitions.mark_metric_as_merged(metric_name=metric_name, measure_name=None)
            refactored = True
            refactor_logs.append((f"Added ratio metric '{metric_name}' to model '{model_node['name']}'.", metric_name))
            moved_to_model = True

        return refactored, refactor_logs, moved_to_model

    raise ValueError(f"Unknown metric type: {metric['type']}")


def merge_complex_metrics_with_model(
    model_node: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Tuple[CommentedMap, bool, List[Tuple[str, Optional[str]]]]:
    refactored = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []
    semantic_model = semantic_definitions.get_semantic_model(model_node["name"])
    if not semantic_model:
        return model_node, refactored, refactor_logs

    for metric_name, metric in semantic_definitions.initial_metrics.items():
        if metric_name in semantic_definitions.merged_metrics:
            continue

        metric_refactored, metric_refactor_logs, _is_on_model = try_to_merge_complex_metric_with_model_recursive(
            metric,
            model_node,
            semantic_model,
            semantic_definitions,
        )
        refactored = refactored or metric_refactored
        refactor_logs.extend(metric_refactor_logs)

    return model_node, refactored, refactor_logs


def add_metric_for_measures_in_model(
    model_node: CommentedMap,
    semantic_definitions: SemanticDefinitions,
) -> Tuple[CommentedMap, bool, List[Tuple[str, Optional[str]]]]:
    """Add metrics for the measures in a semantic model."""
    refactored = False
    refactor_logs: List[Tuple[str, Optional[str]]] = []

    semantic_model = semantic_definitions.get_semantic_model(model_node["name"])

    def create_simple_metric_from_measure(measure: CommentedMap, is_hidden: bool) -> Tuple[CommentedMap, bool]:
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
        if measure_name in semantic_definitions.initial_metrics:
            continue
        elif semantic_definitions.artificial_metric_name_exists(measure_name):
            continue
        elif measure.get("create_metric", False):
            metric, is_new_metric = create_simple_metric_from_measure(measure, is_hidden=False)
        else:
            if semantic_definitions.measure_is_merged(measure_name):
                continue
            metric, is_new_metric = create_simple_metric_from_measure(measure, is_hidden=True)

        if is_new_metric:
            refactored = True
            refactor_logs.append(
                (f"Added simple metric '{metric.get('name')}' to model '{model_node['name']}'.", metric.get("name"))
            )

    return model_node, refactored, refactor_logs


def merge_semantic_models_with_model(
    node: CommentedMap, semantic_definitions: SemanticDefinitions
) -> Tuple[CommentedMap, bool, List[str]]:
    refactored = False
    refactor_logs: List[str] = []

    if "versions" in node:
        pass
    elif semantic_model := semantic_definitions.get_semantic_model(node["name"]):
        node_logs = []
        semantic_model_block = {
            "enabled": True,
        }
        if semantic_model.get("config"):
            semantic_model_block["config"] = semantic_model["config"]
        if semantic_model["name"] != node["name"]:
            semantic_model_block["name"] = node["name"]
        node["semantic_model"] = semantic_model_block

        if semantic_model.get("description"):
            if node.get("description"):
                node["description"] += f" {semantic_model['description']}"
                node_logs.append("Appended semantic model 'description' to model 'description'.")
            else:
                node["description"] = semantic_model["description"]
                node_logs.append("Set model 'description' to semantic model 'description'.")

        if agg_time_dimension := get_dict(semantic_model, "defaults").get("agg_time_dimension"):
            node["agg_time_dimension"] = agg_time_dimension
            node_logs.append("Set model 'agg_time_dimension' to semantic model 'agg_time_dimension'.")

        node_logs.extend(merge_entities_with_model_columns(node, get_list(semantic_model, "entities")))
        node_logs.extend(merge_dimensions_with_model_columns(node, get_list(semantic_model, "dimensions")))

        refactored = True
        refactor_log = f"Model '{node['name']}' - Merged with semantic model '{semantic_model['name']}'."
        semantic_definitions.mark_semantic_model_as_merged(semantic_model["name"], node["name"])
        for log in node_logs:
            refactor_log += f"\n\t* {log}"
        refactor_logs.append(refactor_log)

    return node, refactored, refactor_logs


# ---------------------------------------------------------------------------
# Implementation classes
# ---------------------------------------------------------------------------


class _MergeSimpleMetricsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]
        self._refactor_entries: List[RefactorEntry] = []
        self._refactored = False

    def _process_model(self, node: CommentedMap, index: int, yml_dict: CommentedMap) -> None:
        processed_node, node_refactored, node_refactor_logs = combine_simple_metrics_with_their_input_measure(
            node, self.semantic_definitions
        )
        if node_refactored:
            self._refactored = True
            yml_dict["models"][index] = processed_node
            for log, metric_name in node_refactor_logs:
                orig_metric = self.semantic_definitions.initial_metrics.get(metric_name) if metric_name else None
                original_location = location_of_node(orig_metric) if orig_metric is not None else None
                r = DbtDeprecationRefactor(
                    log=log,
                    deprecation=None,
                    change_type=ChangeType.SIMPLE_METRICS_MERGED_WITH_MODEL,
                    original_location=original_location,
                )

                def resolve(parsed, refactor=r, model_index=index, metric_name=metric_name):
                    model = get_list(parsed, "models")[model_index]
                    if metric_name:
                        metric = next(
                            (m for m in get_list(model, "metrics") if m.get("name") == metric_name),
                            None,
                        )
                        if metric is not None:
                            refactor.edited_location = location_of_node(metric)
                            return
                    refactor.edited_location = location_of_node(model)

                self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        for i, node in enumerate(get_list(yml_dict, "models")):
            self._process_model(node, i, yml_dict)
        return YMLRuleRefactorResult(
            rule_name="merge_simple_metrics_with_model_metrics",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )


class _MergeComplexMetricsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]
        self._refactor_entries: List[RefactorEntry] = []
        self._refactored = False

    def _process_model(self, node: CommentedMap, index: int, yml_dict: CommentedMap) -> None:
        processed_node, node_refactored, node_refactor_logs = merge_complex_metrics_with_model(
            node, self.semantic_definitions
        )
        if node_refactored:
            self._refactored = True
            yml_dict["models"][index] = processed_node
            for log, metric_name in node_refactor_logs:
                orig_metric = self.semantic_definitions.initial_metrics.get(metric_name) if metric_name else None
                original_location = location_of_node(orig_metric) if orig_metric is not None else None
                r = DbtDeprecationRefactor(
                    log=log,
                    deprecation=None,
                    change_type=ChangeType.COMPLEX_METRICS_MERGED_WITH_MODEL,
                    original_location=original_location,
                )

                def resolve(parsed, refactor=r, model_index=index, metric_name=metric_name):
                    model = get_list(parsed, "models")[model_index]
                    if metric_name:
                        metric = next(
                            (m for m in get_list(model, "metrics") if m.get("name") == metric_name),
                            None,
                        )
                        if metric is not None:
                            refactor.edited_location = location_of_node(metric)
                            return
                    refactor.edited_location = location_of_node(model)

                self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        for i, node in enumerate(get_list(yml_dict, "models")):
            self._process_model(node, i, yml_dict)
        return YMLRuleRefactorResult(
            rule_name="merge_complex_metrics_with_model_metrics",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )


class _AddMetricsForMeasuresImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]
        self._refactor_entries: List[RefactorEntry] = []
        self._refactored = False

    def _process_model(self, node: CommentedMap, index: int, yml_dict: CommentedMap) -> None:
        processed_node, node_refactored, node_refactor_logs = add_metric_for_measures_in_model(
            node, self.semantic_definitions
        )
        if node_refactored:
            self._refactored = True
            yml_dict["models"][index] = processed_node
            for log, metric_name in node_refactor_logs:
                orig_metric = self.semantic_definitions.initial_metrics.get(metric_name) if metric_name else None
                original_location = location_of_node(orig_metric) if orig_metric is not None else None
                r = DbtDeprecationRefactor(
                    log=log,
                    deprecation=None,
                    change_type=ChangeType.METRICS_ADDED_FOR_MEASURES,
                    original_location=original_location,
                )

                def resolve(parsed, refactor=r, model_index=index, metric_name=metric_name):
                    model = get_list(parsed, "models")[model_index]
                    if metric_name:
                        metric = next(
                            (m for m in get_list(model, "metrics") if m.get("name") == metric_name),
                            None,
                        )
                        if metric is not None:
                            refactor.edited_location = location_of_node(metric)
                            return
                    refactor.edited_location = location_of_node(model)

                self._refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        for i, node in enumerate(get_list(yml_dict, "models")):
            self._process_model(node, i, yml_dict)
        return YMLRuleRefactorResult(
            rule_name="add_new_metrics_for_measures_to_model",
            refactored=self._refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict) if self._refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=self._refactor_entries,
        )


class _MergeSemanticModelsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]
        self._refactored = False

    def execute(self) -> YMLRuleRefactorResult:
        yml_str = self.yml_str
        semantic_definitions = self.semantic_definitions
        refactored = False
        refactor_entries: List[RefactorEntry] = []
        yml_dict = load_yaml(yml_str)
        models = get_list(yml_dict, "models")
        original_models_count = len(models)
        new_model_count = 0

        last_idx = len(models) - 1
        for i, node in enumerate(models):
            # Rebalance: save the deep-trailing above-comment before modifying the last model.
            above = extract_deep_trailing_above_comment(models) if i == last_idx else None

            original_location = location_of_node(node)
            processed_node, node_refactored, node_refactor_logs = merge_semantic_models_with_model(
                node, semantic_definitions
            )

            if node_refactored:
                refactored = True
                yml_dict["models"][i] = processed_node
                # Rebalance: re-insert the saved above-comment at the new deepest trailing position.
                if above is not None:
                    insert_at_deep_trailing(models, above)
                for log in node_refactor_logs:
                    r = DbtDeprecationRefactor(
                        log=log,
                        deprecation=None,
                        change_type=ChangeType.SEMANTIC_MODEL_MERGED_WITH_MODEL,
                        original_location=original_location,
                    )

                    def resolve(parsed, refactor=r, model_index=i):
                        refactor.edited_location = location_of_node(get_list(parsed, "models")[model_index])

                    refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))

        sem_models_seq = get_list(yml_dict, "semantic_models")
        for sem_idx, semantic_model in enumerate(sem_models_seq):
            model_key = semantic_definitions.get_model_key_for_semantic_model(semantic_model)
            if model_key and not semantic_definitions.model_key_exists_for_semantic_model(model_key):
                if "models" not in yml_dict:
                    yml_dict["models"] = CommentedSeq()
                    # Transfer the semantic_models: key inline comment to models: if present.
                    if "semantic_models" in yml_dict.ca.items:
                        yml_dict.ca.items["models"] = copy.deepcopy(yml_dict.ca.items["semantic_models"])

                new_model_node = CommentedMap({"name": model_key[0]})
                # Transfer the name: key inline comment from the semantic model to preserve annotations.
                sem_name_node = extract_node(semantic_model, "name")
                if sem_name_node.comments is not None:
                    new_model_node.ca.items["name"] = sem_name_node.comments

                processed_new_model_node, new_model_node_refactored, new_model_node_refactor_logs = (
                    merge_semantic_models_with_model(new_model_node, semantic_definitions)
                )
                if new_model_node_refactored:
                    refactored = True
                    # Capture seq-item comment from the semantic_models list to carry to models list.
                    sem_seq_node = extract_node(sem_models_seq, sem_idx)
                    # Rebalance: save before appending (appending shifts the deep trailing).
                    above = extract_deep_trailing_above_comment(yml_dict["models"])
                    append_node(
                        yml_dict["models"],
                        Node(
                            value=processed_new_model_node,
                            original_location=sem_seq_node.original_location,
                            comments=sem_seq_node.comments,
                        ),
                    )
                    if above is not None:
                        insert_at_deep_trailing(yml_dict["models"], above)
                    edited_model_index = original_models_count + new_model_count
                    new_model_count += 1
                    original_location = location_of_node(semantic_model)
                    for log in new_model_node_refactor_logs:
                        r = DbtDeprecationRefactor(
                            log=log,
                            deprecation=None,
                            change_type=ChangeType.SEMANTIC_MODEL_MERGED_WITH_MODEL,
                            original_location=original_location,
                        )

                        def resolve(parsed, refactor=r, model_index=edited_model_index):
                            refactor.edited_location = location_of_node(get_list(parsed, "models")[model_index])

                        refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))

        return YMLRuleRefactorResult(
            rule_name="restructure_owner_properties",
            refactored=refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict) if refactored else yml_str,
            original_yaml=yml_str,
            refactor_entries=refactor_entries,
        )


class _DeleteTopLevelSemanticModelsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        top_level_semantic_models = get_list(yml_dict, "semantic_models")
        new_semantic_models = []
        refactor_entries: List[RefactorEntry] = []
        refactored = False

        orig_semantic_models = {sm["name"]: sm for sm in get_list(self.content.original_parsed, "semantic_models")}

        for semantic_model in top_level_semantic_models:
            if semantic_model["name"] in self.semantic_definitions.merged_semantic_models:
                refactored = True
                refactor_entries.append(
                    RefactorEntry(
                        refactor=DbtDeprecationRefactor(
                            log=f"Deleted top-level semantic model '{semantic_model['name']}'.",
                            deprecation=None,
                            change_type=ChangeType.TOP_LEVEL_SEMANTIC_MODEL_DELETED,
                            original_location=location_of_node(orig_semantic_models[semantic_model["name"]])
                            if semantic_model["name"] in orig_semantic_models
                            else None,
                        )
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
            refactored_yaml=dict_to_yaml_str(yml_dict, write_empty=True) if refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=refactor_entries,
        )


class _MigrateMetricTagsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        metrics = get_list(yml_dict, "metrics")
        transformed_metrics = []
        refactor_entries: List[RefactorEntry] = []
        refactored = False
        original_metrics = {m["name"]: m for m in get_list(self.content.original_parsed, "metrics")}

        for metric in metrics:
            if deprecated_tags := metric.pop("tags", None):
                if not isinstance(deprecated_tags, list):
                    break
                metric_config = get_dict(metric, "config")
                metric_tags = get_list(metric_config, "tags")
                if isinstance(metric_tags, str):
                    metric_tags = [metric_tags]
                deprecated_tags.extend(metric_tags)
                metric_config["tags"] = deprecated_tags
                metric["config"] = metric_config
                refactored = True
                orig_metric_node = original_metrics.get(metric["name"])
                r = DbtDeprecationRefactor(
                    log=f"Migrated metric '{metric['name']}' tags field to config.",
                    deprecation=None,
                    change_type=ChangeType.METRIC_TAGS_MIGRATION,
                    original_location=location_of_key(orig_metric_node, "tags") if orig_metric_node else None,
                )
                metric_name = metric["name"]

                def resolve(parsed, refactor=r, name=metric_name):
                    for model in get_list(parsed, "models"):
                        for m in get_list(model, "metrics"):
                            if m.get("name") == name:
                                refactor.edited_location = location_of_key(get_dict(m, "config"), "tags")
                                return

                refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))
                self.semantic_definitions.initial_metrics[metric["name"]] = metric
            transformed_metrics.append(metric)

        if refactored:
            yml_dict["metrics"] = transformed_metrics

        return YMLRuleRefactorResult(
            rule_name="migrate_metric_tags_field_to_config",
            refactored=refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict, write_empty=True) if refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=refactor_entries,
        )


class _MigrateOrDeleteTopLevelMetricsImpl:
    def __init__(self, content: YMLContent, config: YMLRefactorConfig) -> None:
        self.content = content
        self.yml_str = content.current_str
        self.semantic_definitions: SemanticDefinitions = config.semantic_definitions  # type: ignore[assignment]

    def execute(self) -> YMLRuleRefactorResult:
        yml_dict = load_yaml(self.yml_str)
        metric_locations = {m["name"]: location_of_node(m) for m in get_list(self.content.original_parsed, "metrics")}
        raw_metrics = yml_dict.get("metrics") or []
        top_level_metrics = sorted(raw_metrics, key=lambda x: x.get("name"))
        transformed_metrics = []
        refactor_entries: List[RefactorEntry] = []
        refactored = False
        transformed_index = 0

        for metric in top_level_metrics:
            if metric["name"] in self.semantic_definitions.merged_metrics:
                refactored = True
                refactor_entries.append(
                    RefactorEntry(
                        refactor=DbtDeprecationRefactor(
                            log=f"Deleted top-level metric '{metric['name']}'.",
                            deprecation=None,
                            change_type=ChangeType.TOP_LEVEL_METRIC_DELETED,
                            original_location=metric_locations.get(metric["name"]),
                        )
                    )
                )
            else:
                if metric["type"] == "conversion":
                    type_params = metric.pop("type_params", {})
                    conversion_type_params = type_params.pop("conversion_type_params", {})
                    base_measure_input = MeasureInput.parse_from_yaml(conversion_type_params.pop("base_measure", None))
                    new_base_metric = (
                        self.semantic_definitions.get_artificial_metric(
                            measure_name=base_measure_input.name,
                            fill_nulls_with=base_measure_input.fill_nulls_with,
                            join_to_timespine=base_measure_input.join_to_timespine,
                        )
                        if base_measure_input
                        else None
                    )
                    if base_measure_input and new_base_metric:
                        metric["base_metric"] = base_measure_input.to_metric_input_yaml_obj(
                            metric_name=new_base_metric["name"]
                        )

                    conversion_measure_input = MeasureInput.parse_from_yaml(
                        conversion_type_params.pop("conversion_measure", None)
                    )
                    new_conversion_metric = (
                        self.semantic_definitions.get_artificial_metric(
                            measure_name=conversion_measure_input.name,
                            fill_nulls_with=conversion_measure_input.fill_nulls_with,
                            join_to_timespine=conversion_measure_input.join_to_timespine,
                        )
                        if conversion_measure_input
                        else None
                    )
                    if conversion_measure_input and new_conversion_metric:
                        metric["conversion_metric"] = conversion_measure_input.to_metric_input_yaml_obj(
                            metric_name=new_conversion_metric["name"],
                        )

                    metric.update(conversion_type_params)
                    metric.update(type_params)
                else:
                    type_params = metric.pop("type_params", {})
                    metric.update(type_params)
                    change_metrics_to_input_metrics(metric)

                transformed_metrics.append(metric)
                refactored = True
                r = DbtDeprecationRefactor(
                    log=f"Updated top-level metric '{metric['name']}' to be compatible with new syntax, but left at top-level.",
                    deprecation=None,
                    change_type=ChangeType.TOP_LEVEL_METRIC_UPDATED,
                    original_location=metric_locations.get(metric["name"]),
                )

                def resolve(parsed, refactor=r, metric_index=transformed_index):
                    refactor.edited_location = location_of_node(get_list(parsed, "metrics")[metric_index])

                refactor_entries.append(RefactorEntry(refactor=r, resolve=resolve))
                transformed_index += 1

        if not transformed_metrics:
            yml_dict.pop("metrics", None)
        else:
            yml_dict["metrics"] = transformed_metrics

        return YMLRuleRefactorResult(
            rule_name="migrate_or_delete_top_level_metrics",
            refactored=refactored,
            refactored_yaml=dict_to_yaml_str(yml_dict, write_empty=True) if refactored else self.yml_str,
            original_yaml=self.yml_str,
            refactor_entries=refactor_entries,
        )
