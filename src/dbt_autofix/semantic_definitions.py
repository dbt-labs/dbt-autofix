import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ruamel.yaml.comments import CommentedMap, CommentedSeq

from dbt_autofix.jinja import statically_parse_ref
from dbt_autofix.refactors.yml import (
    ProjectYamlCache,
    get_list,
    iter_project_yaml_files,
    load_yaml,
)

logger = logging.getLogger(__name__)


def _as_top_level_yaml_list(yml_dict: CommentedMap, key: str) -> list[Any]:
    """Top-level `semantic_models` / `models` / `metrics` must be sequences, not a mapping (which would `list` keys)."""
    v = yml_dict.get(key)
    if v is None:
        return []
    if isinstance(v, (str, bytes, int, float, bool)):
        return []
    if isinstance(v, (dict, CommentedMap)):
        return []
    if isinstance(v, (list, tuple, CommentedSeq)):
        return list(v)
    if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
        return list(v)
    return []


def _is_mapping_node(node: Any) -> bool:
    return isinstance(node, (CommentedMap, dict))


def _coerce_semantic_model_node(node: Any) -> Optional[CommentedMap]:
    """Store only ``CommentedMap`` in the semantic model index; coerce plain ``dict`` from atypical parses."""
    if isinstance(node, CommentedMap):
        return node
    if isinstance(node, dict):
        c = CommentedMap()
        for k, v in node.items():
            c[k] = v
        return c
    return None


class SemanticDefinitions:
    def __init__(
        self,
        root_path: Path,
        dbt_paths: List[str],
        *,
        yaml_cache: "ProjectYamlCache | None" = None,
    ):
        # All semantic models from semantic_models: entries in schema.yml files, keyed by their model key
        # All model keys from models: entries in schema.yml files; top-level metrics from metrics:
        if yaml_cache is not None:
            self.semantic_models, self.model_yml_keys, self.initial_metrics = self._collect_from_yaml_cache(yaml_cache)
        else:
            self.semantic_models, self.model_yml_keys, self.initial_metrics = self._collect_from_project_yaml(
                root_path, dbt_paths
            )

        self.merged_semantic_models: Set[str] = set()
        self._semantic_model_to_dbt_model_name_map: Dict[str, str] = {}

        self.merged_metrics: Set[str] = set()
        # these are measures that have been merged into a metric of SOME sort.  They may not exist in an obvious
        # way; for example, it might have been folded up into an existing simple metric.
        self._merged_measures: Set[str] = set()
        # Simple metrics created just to replace an old measure.  This maps
        # (measure_name, fill_nulls_with value, join_to_timespine value) to the new metric
        # to help power deduplication lookups.
        self._artificial_metric_names_map: Dict[Tuple[str, Optional[str], Optional[bool]], CommentedMap] = {}
        self._set_of_artificial_metric_names: Set[str] = set()

    def get_semantic_model(self, dbt_model_name: str, version: Optional[str] = None) -> CommentedMap:
        model_key = (dbt_model_name, version)
        return self.semantic_models.get(model_key) or CommentedMap()

    def get_model_key_for_semantic_model(self, semantic_model: CommentedMap) -> Optional[Tuple[str, Optional[str]]]:
        """Resolve the ``(model_name, version)`` key; requires a parseable ``model:`` (``ref``) and non-empty name."""
        expr = semantic_model.get("model")
        if expr is None or expr == "":
            return None
        if not isinstance(expr, str):
            expr = str(expr)
        try:
            ref = statically_parse_ref(expr)
        except Exception:
            return None
        if not ref or not ref.name or not str(ref.name).strip():
            return None
        return (str(ref.name), ref.version)

    def model_key_exists_for_semantic_model(self, model_key: Tuple[str, Optional[str]]) -> bool:
        return model_key in self.model_yml_keys

    def mark_metric_as_merged(self, metric_name: str, measure_name: Optional[str]):
        self.merged_metrics.add(metric_name)
        if measure_name:
            self._merged_measures.add(measure_name)

    def measure_is_merged(self, measure_name: str) -> bool:
        return measure_name in self._merged_measures

    def mark_semantic_model_as_merged(self, semantic_model_name: str, new_dbt_model_name: str):
        self.merged_semantic_models.add(semantic_model_name)
        self._semantic_model_to_dbt_model_name_map[semantic_model_name] = new_dbt_model_name

    def record_artificial_metric(
        self,
        *,
        measure_name: str,
        fill_nulls_with: Optional[str],
        join_to_timespine: Optional[bool],
        metric: CommentedMap,
    ):
        self._artificial_metric_names_map[(measure_name, fill_nulls_with, join_to_timespine)] = metric
        name = metric.get("name")
        if name is not None:
            self._set_of_artificial_metric_names.add(str(name) if not isinstance(name, str) else name)

    def get_artificial_metric(
        self,
        *,
        measure_name: str,
        fill_nulls_with: Optional[str],
        join_to_timespine: Optional[bool],
    ) -> Optional[CommentedMap]:
        return self._artificial_metric_names_map.get((measure_name, fill_nulls_with, join_to_timespine))

    def artificial_metric_name_exists(self, metric_name: str) -> bool:
        return metric_name in self._set_of_artificial_metric_names

    def get_model_for_semantic_model(self, semantic_model_name: str) -> str:
        return self._semantic_model_to_dbt_model_name_map[semantic_model_name]

    @staticmethod
    def _merge_yml_dict_into_semantic_indexes(
        yml_dict: CommentedMap,
        semantic_models: Dict[Tuple[str, Optional[str]], CommentedMap],
        model_yml_keys: Set[Tuple[str, Optional[str]]],
        metrics: Dict[str, CommentedMap],
    ) -> None:
        for sm in _as_top_level_yaml_list(yml_dict, "semantic_models"):
            if not _is_mapping_node(sm):
                continue
            expr = sm.get("model")
            if expr in (None, ""):
                continue
            if not isinstance(expr, str):
                expr = str(expr)
            try:
                ref = statically_parse_ref(expr)
            except Exception:
                continue
            if ref and ref.name and str(ref.name).strip():
                sm_node = _coerce_semantic_model_node(sm)
                if sm_node is not None:
                    name_key = str(ref.name)
                    semantic_models[(name_key, ref.version)] = sm_node

        for model in _as_top_level_yaml_list(yml_dict, "models"):
            if not _is_mapping_node(model):
                continue
            mname = model.get("name")
            if mname is None or (isinstance(mname, str) and not mname.strip()):
                continue
            mname = str(mname) if not isinstance(mname, str) else mname
            if not model.get("versions"):
                model_yml_keys.add((mname, None))
            else:
                for ver in _as_top_level_yaml_list(model, "versions"):
                    if not _is_mapping_node(ver):
                        continue
                    v_tag = ver.get("v")
                    if v_tag is not None and not isinstance(v_tag, str):
                        v_tag = str(v_tag)
                    model_yml_keys.add((mname, v_tag))

        for metric in _as_top_level_yaml_list(yml_dict, "metrics"):
            if not _is_mapping_node(metric):
                continue
            metric_name = metric.get("name")
            if metric_name is None:
                continue
            if isinstance(metric_name, str) and not metric_name.strip():
                continue
            key = str(metric_name) if not isinstance(metric_name, str) else metric_name
            metrics[key] = metric

    @staticmethod
    def _collect_from_yaml_cache(
        yaml_cache: "ProjectYamlCache",
    ) -> tuple[Dict[Tuple[str, Optional[str]], CommentedMap], Set[Tuple[str, Optional[str]]], Dict[str, CommentedMap]]:
        """Build indexes from pre-parsed project YAMLs (``build_project_yaml_cache`` in semantic mode)."""
        semantic_models: Dict[Tuple[str, Optional[str]], CommentedMap] = {}
        model_yml_keys: Set[Tuple[str, Optional[str]]] = set()
        metrics: Dict[str, CommentedMap] = {}
        for yml_file in yaml_cache.ordered_paths:
            try:
                yml_dict = yaml_cache.parsed_by_path.get(yml_file) or CommentedMap()
                SemanticDefinitions._merge_yml_dict_into_semantic_indexes(
                    yml_dict, semantic_models, model_yml_keys, metrics
                )
            except Exception as e:
                logger.warning(
                    "Skipping semantic index merge for %s: %s: %s",
                    yml_file,
                    type(e).__name__,
                    e,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )
        return semantic_models, model_yml_keys, metrics

    @staticmethod
    def _collect_from_project_yaml(
        root_path: Path, dbt_paths: List[str]
    ) -> tuple[Dict[Tuple[str, Optional[str]], CommentedMap], Set[Tuple[str, Optional[str]]], Dict[str, CommentedMap]]:
        """One glob over dbt model paths, one read + parse per file (same as cache build), dispatch into indexes."""
        semantic_models: Dict[Tuple[str, Optional[str]], CommentedMap] = {}
        model_yml_keys: Set[Tuple[str, Optional[str]]] = set()
        metrics: Dict[str, CommentedMap] = {}

        for yml_file in iter_project_yaml_files(root_path, dbt_paths):
            try:
                yml_dict = load_yaml(yml_file)
            except Exception as e:
                logger.warning(
                    "Could not load YAML for semantic index (skipping %s): %s: %s",
                    yml_file,
                    type(e).__name__,
                    e,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )
                continue
            try:
                SemanticDefinitions._merge_yml_dict_into_semantic_indexes(
                    yml_dict, semantic_models, model_yml_keys, metrics
                )
            except Exception as e:
                logger.warning(
                    "Could not build semantic index from %s: %s: %s",
                    yml_file,
                    type(e).__name__,
                    e,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )
        return semantic_models, model_yml_keys, metrics


@dataclass
class MeasureInput:
    name: str
    _is_object_form: bool
    fill_nulls_with: Optional[str] = None
    join_to_timespine: Optional[bool] = None
    filter: Optional[str] = None
    alias: Optional[str] = None

    @staticmethod
    def parse_from_yaml(yaml_obj: Optional[Union[str, CommentedMap]]) -> Optional["MeasureInput"]:
        if yaml_obj is None:
            return None
        if isinstance(yaml_obj, dict):
            return MeasureInput(
                name=yaml_obj["name"],
                _is_object_form=True,
                fill_nulls_with=yaml_obj.get("fill_nulls_with"),
                join_to_timespine=yaml_obj.get("join_to_timespine"),
                filter=yaml_obj.get("filter"),
                alias=yaml_obj.get("alias"),
            )
        else:
            return MeasureInput(name=yaml_obj, _is_object_form=False)

    def to_metric_input_yaml_obj(self, metric_name: str) -> Union[str, Dict[str, Any]]:
        """Convert to a metric input object.  Several fields will be lost in the process."""
        if not self._is_object_form:
            return metric_name

        inputs = {
            "name": metric_name,
            "filter": self.filter,
            "alias": self.alias,
        }
        # filter out fields that did not exist before
        inputs = {k: v for k, v in inputs.items() if v is not None}
        return inputs


class ModelAccessHelpers:
    @staticmethod
    def get_measures_from_model(semantic_model_node: CommentedMap) -> List[CommentedMap]:
        return get_list(semantic_model_node, "measures")

    @staticmethod
    def maybe_get_measure_from_model(
        semantic_model_node: CommentedMap,
        measure_name: str,
    ) -> Optional[CommentedMap]:
        all_measures = ModelAccessHelpers.get_measures_from_model(semantic_model_node)
        return next(
            (m for m in all_measures if m["name"] == measure_name),
            None,
        )
