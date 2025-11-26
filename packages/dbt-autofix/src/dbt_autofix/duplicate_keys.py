from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import yaml
import yamllint.config
import yamllint.linter
from rich.console import Console

from dbt_autofix.refactors.yml import DbtYAML

console = Console()

config = """
rules:
  key-duplicates: enable
"""

yaml_config = yamllint.config.YamlLintConfig(config)


@dataclass
class DuplicateFound:
    file: Path
    line: int
    key: str
    value: str

    def __str__(self):
        return f"{self.file}:{self.line} -- {self.value}"


def find_duplicate_keys(
    root_dir: Path,
    dry_run: bool = False,
) -> Tuple[List[DuplicateFound], List[DuplicateFound]]:
    """
    Find duplicate keys in the project and packages.
    """
    project_duplicates: List[DuplicateFound] = []
    package_duplicates: List[DuplicateFound] = []

    yml_files = set(root_dir.glob("**/*.yml")).union(set(root_dir.glob("**/*.yaml")))
    yml_files_target = set((root_dir / "target").glob("**/*.yml")).union(set((root_dir / "target").glob("**/*.yaml")))

    packages_path = yaml.safe_load((root_dir / "dbt_project.yml").read_text()).get(
        "packages-install-path", "dbt_packages"
    )

    yml_files_packages = set((root_dir / packages_path).glob("**/*.yml")).union(
        set((root_dir / packages_path).glob("**/*.yaml"))
    )

    # this is a hack to avoid checking integration_tests. it won't work everywhere but it's good enough for now
    yml_files_packages_integration_tests = set((root_dir / packages_path).glob("**/integration_tests/**/*.yml")).union(
        set((root_dir / packages_path).glob("**/integration_tests/**/*.yaml"))
    )
    yml_files_packages_not_integration_tests = yml_files_packages - yml_files_packages_integration_tests

    yml_files_not_target_or_packages = yml_files - yml_files_target - yml_files_packages

    # Check project YML files
    for file in yml_files_not_target_or_packages:
        file_with_duplicate = False
        file_content = file.read_text()
        for p in yamllint.linter.run(file_content, yaml_config):
            if p.rule == "key-duplicates":
                file_with_duplicate = True
                project_duplicates.append(
                    DuplicateFound(
                        file=file,
                        line=p.line,
                        key=p.desc.split('"')[1] if '"' in p.desc else "",  # Extract key from description
                        value=p.desc,
                    )
                )
        if file_with_duplicate and not dry_run:
            without_duplicates = yaml.safe_load(file_content)
            ruamel_yaml = DbtYAML()
            ruamel_yaml.dump_to_string(without_duplicates)  # type: ignore

    # Check package YML files
    for file in yml_files_packages_not_integration_tests:
        file_content = file.read_text()
        for p in yamllint.linter.run(file_content, yaml_config):
            if p.rule == "key-duplicates":
                package_duplicates.append(
                    DuplicateFound(
                        file=file,
                        line=p.line,
                        key=p.desc.split('"')[1] if '"' in p.desc else "",  # Extract key from description
                        value=p.desc,
                    )
                )

    return project_duplicates, package_duplicates


def print_duplicate_keys(project_duplicates: List[DuplicateFound], package_duplicates: List[DuplicateFound]) -> None:
    """Print duplicate keys in the project and packages as well as instructions to fix them."""
    if not project_duplicates and not package_duplicates:
        return

    if project_duplicates:
        console.print("\nThere are issues in your project YML files", style="bold red")
        console.print(
            (
                "Please remove duplicates by hand. dbt's default behavior is to keep the last occurence of a key.\n"
                "If you want to keep the same behaviour remove or comments lines found for the same key and before in the file.\n"
                "Once you have done all the changes in the files, run the tool again.\n"
            )
        )
        for dup in project_duplicates:
            console.print(str(dup))

    if package_duplicates:
        console.print(
            (
                "\nThose packages might have issues. If those are not maintained by you, check if there are updates available. "
                "If they are private packages, remove duplicates in their own repository and publish a new version.\n"
            ),
            style="bold red",
        )
        for dup in package_duplicates:
            console.print(str(dup))
