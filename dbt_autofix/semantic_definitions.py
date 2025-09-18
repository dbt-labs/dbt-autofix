from typing import Dict, Tuple, Optional, Any, List
from pathlib import Path
from dbt_autofix.refactors.yml import DbtYAML
from dbt_autofix.jinja import statically_parse_ref


class SemanticDefinitions:
    def __init__(self, root_path: Path, dbt_paths: List[str]):
        self.semantic_models: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = self.collect_semantic_models(root_path, dbt_paths)
    
    def get_semantic_model(self, model_name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        model_key = (model_name, version)
        return self.semantic_models.get(model_key)

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
