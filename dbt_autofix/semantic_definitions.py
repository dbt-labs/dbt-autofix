from typing import Dict, Tuple, Optional, Any, List, Set
from pathlib import Path
from dbt_autofix.refactors.yml import DbtYAML
from dbt_autofix.jinja import statically_parse_ref


class SemanticDefinitions:
    def __init__(self, root_path: Path, dbt_paths: List[str]):
        # All semantic models from semantic_models: entries in schema.yml files, keyed by their model key
        self.semantic_models: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = self.collect_semantic_models(root_path, dbt_paths)
        # All model keys from models: entries in schema.yml files
        self.model_yml_keys: Set[Tuple[str, Optional[str]]] = self.collect_model_yml_keys(root_path, dbt_paths)
        # All top-level metrics from metrics: entries in schema.yml files
        self.metrics: Dict[str, Dict[str, Any]] = self.collect_metrics(root_path, dbt_paths)

        self.merged_semantic_models: Set[str] = set()
        self.merged_metrics: Set[str] = set()
    
    def get_semantic_model(self, model_name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        model_key = (model_name, version)
        return self.semantic_models.get(model_key)

    def get_model_key_for_semantic_model(self, semantic_model: Dict[str, Any]) -> Optional[Tuple[str, Optional[str]]]:
        ref = statically_parse_ref(semantic_model["model"])
        if not ref:
            return None
        return (ref.name, ref.version)

    def model_key_exists_for_semantic_model(self, model_key: Tuple[str, Optional[str]]) -> bool:
        return model_key in self.model_yml_keys
    
    def mark_metric_as_merged(self, metric_name: str):
        self.merged_metrics.add(metric_name)
    
    def mark_semantic_model_as_merged(self, semantic_model_name: str):
        self.merged_semantic_models.add(semantic_model_name)

    def collect_semantic_models(self, root_path: Path, dbt_paths: List[str]) -> Dict[Tuple[str, Optional[str]], Dict[str, Any]]:
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