from typing import Dict, Tuple, Optional, Any, List, Set
from pathlib import Path
from dbt_autofix.refactors.yml import DbtYAML
from dbt_autofix.jinja import statically_parse_ref


class SemanticDefinitions:
    def __init__(self, root_path: Path, dbt_paths: List[str]):
        # All semantic models from semantic_models: entries in schema.yml files, keyed by their model key
        self.semantic_models: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = self.collect_semantic_models(
            root_path, dbt_paths
        )
        # All model keys from models: entries in schema.yml files
        self.model_yml_keys: Set[Tuple[str, Optional[str]]] = self.collect_model_yml_keys(root_path, dbt_paths)
        # All top-level metrics from metrics: entries in schema.yml files
        self.initial_metrics: Dict[str, Dict[str, Any]] = self.collect_metrics(root_path, dbt_paths)

        self.merged_semantic_models: Set[str] = set()
        self._semantic_model_to_dbt_model_name_map: Dict[str, str] = {}

        # TODO @patricky do I need this?
        self.merged_metrics: Set[str] = set()
        # these are measures that have been merged into a metric of SOME sort.  They may not exist in an obvious 
        # way; for example, it might have been folded up into an existing simple metric.
        self._merged_measures: Set[str] = set()
        # Simple metrics created just to replace an old measure.  This maps 
        # (measure_name, fill_nulls_with value, join_to_timespine value) to the new metric
        # to help power deduplication lookups.
        self._artificial_metric_names_map: Dict[Tuple[str, Optional[str], Optional[bool]], Dict[str, Any]] = {}
        self._set_of_artificial_metric_names: Set[str] = set()
    
    def get_semantic_model(self, dbt_model_name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        model_key = (dbt_model_name, version)
        return self.semantic_models.get(model_key)

    def get_model_key_for_semantic_model(self, semantic_model: Dict[str, Any]) -> Optional[Tuple[str, Optional[str]]]:
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
        metric: Dict[str, Any],
    ):
        self._artificial_metric_names_map[(measure_name, fill_nulls_with, join_to_timespine)] = metric
        self._set_of_artificial_metric_names.add(metric["name"])

    def get_artificial_metric(
        self,
        *,
        measure_name: str,
        fill_nulls_with: Optional[str],
        join_to_timespine: Optional[bool],
    ) -> Optional[Dict[str, Any]]:
        return self._artificial_metric_names_map.get((measure_name, fill_nulls_with, join_to_timespine))
    
    def artificial_metric_name_exists(self, metric_name: str) -> bool:
        return metric_name in self._set_of_artificial_metric_names

    def get_model_for_semantic_model(self, semantic_model_name: str) -> str:
        return self._semantic_model_to_dbt_model_name_map[semantic_model_name]

    def collect_semantic_models(
        self, root_path: Path, dbt_paths: List[str]
    ) -> Dict[Tuple[str, Optional[str]], Dict[str, Any]]:
        semantic_models: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                yml_str = yml_file.read_text()
                yml_dict = DbtYAML().load(yml_str) or {}
                if "semantic_models" in yml_dict:
                    for semantic_model in yml_dict["semantic_models"]:
                        ref = statically_parse_ref(semantic_model["model"])
                        if ref:
                            semantic_models[(ref.name, ref.version)] = semantic_model
        return semantic_models

    def collect_model_yml_keys(self, root_path: Path, dbt_paths: List[str]) -> Set[Tuple[str, Optional[str]]]:
        model_keys: Set[Tuple[str, Optional[str]]] = set()
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                yml_str = yml_file.read_text()
                yml_dict = DbtYAML().load(yml_str) or {}
                if "models" in yml_dict:
                    for model in yml_dict["models"]:
                        if not model.get("versions"):
                            model_keys.add((model["name"], None))
                        else:
                            for version in model["versions"]:
                                model_keys.add((model["name"], version.get("v")))
        return model_keys

    def collect_metrics(self, root_path: Path, dbt_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        metrics: Dict[str, Dict[str, Any]] = {}
        for dbt_path in dbt_paths:
            yaml_files = set((root_path / Path(dbt_path)).resolve().glob("**/*.yml")).union(
                set((root_path / Path(dbt_path)).resolve().glob("**/*.yaml"))
            )
            for yml_file in yaml_files:
                yml_str = yml_file.read_text()
                yml_dict = DbtYAML().load(yml_str) or {}
                if "metrics" in yml_dict:
                    for metric in yml_dict["metrics"]:
                        metrics[metric["name"]] = metric
        return metrics
