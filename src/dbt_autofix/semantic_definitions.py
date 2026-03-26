from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ruamel.yaml.comments import CommentedMap

from dbt_autofix.jinja import statically_parse_ref
from dbt_autofix.refactors.yml import get_list, load_yaml


class SemanticDefinitions:
    def __init__(self, root_path: Path, dbt_paths: List[str]):
        # All semantic models from semantic_models: entries in schema.yml files, keyed by their model key
        self.semantic_models, self.initial_measure_seq_comments = self.collect_semantic_models(root_path, dbt_paths)
        # All model keys from models: entries in schema.yml files
        self.model_yml_keys: Set[Tuple[str, Optional[str]]] = self.collect_model_yml_keys(root_path, dbt_paths)
        # All top-level metrics from metrics: entries in schema.yml files
        self.initial_metrics, self.initial_metric_seq_comments = self.collect_metrics(root_path, dbt_paths)

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
        ref = statically_parse_ref(semantic_model["model"])
        if not ref:
            return None
        return (ref.name, ref.version)

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
        self._set_of_artificial_metric_names.add(metric["name"])

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

    def collect_semantic_models(
        self, root_path: Path, dbt_paths: List[str]
    ) -> Tuple[Dict[Tuple[str, Optional[str]], CommentedMap], Dict[str, Optional[list]]]:
        """Returns (semantic_model_key -> semantic_model, measure_name -> seq-item comment)."""
        from dbt_autofix.refactors.node import extract_node

        semantic_models: Dict[Tuple[str, Optional[str]], CommentedMap] = {}
        measure_seq_comments: Dict[str, Optional[list]] = {}
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                yml_dict = load_yaml(yml_file)
                if "semantic_models" in yml_dict:
                    for semantic_model in yml_dict["semantic_models"]:
                        ref = statically_parse_ref(semantic_model["model"])
                        if ref:
                            semantic_models[(ref.name, ref.version)] = semantic_model
                        measures_seq = get_list(semantic_model, "measures")
                        for i, measure in enumerate(measures_seq):
                            measure_seq_comments[measure["name"]] = extract_node(measures_seq, i).comments
        return semantic_models, measure_seq_comments

    def collect_model_yml_keys(self, root_path: Path, dbt_paths: List[str]) -> Set[Tuple[str, Optional[str]]]:
        model_keys: Set[Tuple[str, Optional[str]]] = set()
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                yml_dict = load_yaml(yml_file)
                if "models" in yml_dict:
                    for model in yml_dict["models"]:
                        if not model.get("versions"):
                            model_keys.add((model["name"], None))
                        else:
                            for version in model["versions"]:
                                model_keys.add((model["name"], version.get("v")))
        return model_keys

    def collect_metrics(
        self, root_path: Path, dbt_paths: List[str]
    ) -> Tuple[Dict[str, CommentedMap], Dict[str, Optional[list]]]:
        """Returns (metric_name -> metric, metric_name -> seq-item comment)."""
        from dbt_autofix.refactors.node import extract_node

        metrics: Dict[str, CommentedMap] = {}
        seq_comments: Dict[str, Optional[list]] = {}
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in sorted(yaml_files):
                yml_dict = load_yaml(yml_file)
                if "metrics" in yml_dict:
                    metrics_seq = yml_dict["metrics"]
                    for i, metric in enumerate(metrics_seq):
                        name = metric["name"]
                        metrics[name] = metric
                        seq_comments[name] = extract_node(metrics_seq, i).comments
        return metrics, seq_comments


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
