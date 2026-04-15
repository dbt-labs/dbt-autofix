"""SAO (State Aware Orchestration) configuration module."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console

from dbt_autofix.dbt_api import DBTClient
from dbt_autofix.refactors.changesets.sao_yml import changeset_add_sao_config
from dbt_autofix.refactors.yml import load_yaml

console = Console()


class DiscoveryClient:
    """Client for the dbt Cloud metadata (Discovery) API."""

    def __init__(self, api_key: str, metadata_url: str) -> None:
        self.api_key = api_key
        self.metadata_url = metadata_url

    def get_job_models(self, job_id: int, run_id: int, project_name: str) -> list[str]:
        """Return model names that ran in this job, filtered to the project's own package."""
        query = """
        query ($jobId: BigInt!, $runId: BigInt!) {
          job(id: $jobId, runId: $runId) {
            models { uniqueId name packageName }
          }
        }
        """
        response = httpx.post(
            self.metadata_url,
            json={"query": query, "variables": {"jobId": job_id, "runId": run_id}},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        models = data.get("data", {}).get("job", {}).get("models") or []
        return [m["name"] for m in models if m.get("packageName") == project_name]


def cron_to_build_after(cron: str) -> dict:
    """Convert a cron expression to a build_after dict.

    Examples:
        "9 */12 * * *"  -> {"count": 12, "period": "hour"}
        "*/30 * * * *"  -> {"count": 30, "period": "minute"}
        "0 0 * * *"     -> {"count": 24, "period": "hour"}
        "0 9 * * *"     -> {"count": 24, "period": "hour"}
    """
    parts = cron.strip().split()
    if len(parts) != 5:
        return {"count": 24, "period": "hour"}

    minute, hour = parts[0], parts[1]

    m = re.match(r"^\*/(\d+)$", minute)
    if m and hour == "*":
        return {"count": int(m.group(1)), "period": "minute"}

    m = re.match(r"^\*/(\d+)$", hour)
    if m:
        return {"count": int(m.group(1)), "period": "hour"}

    return {"count": 24, "period": "hour"}


def _build_after_minutes(ba: dict) -> float:
    """Normalize build_after to minutes for frequency comparison."""
    period = ba["period"]
    count = ba["count"]
    if period == "minute":
        return float(count)
    if period == "hour":
        return count * 60.0
    if period == "day":
        return count * 1440.0
    return float("inf")


def metadata_url_from_base(base_url: str) -> str:
    """Derive the metadata API URL from the admin base URL.

    Example: https://tk626.us1.dbt.com -> https://tk626.metadata.us1.dbt.com/graphql
    """
    from urllib.parse import urlparse

    parsed = urlparse(base_url)
    host = parsed.hostname or ""
    first, _, rest = host.partition(".")
    return f"{parsed.scheme}://{first}.metadata.{rest}/graphql"


def resolve_prod_environment(client: DBTClient, project_id: int) -> int:
    """Find the production environment ID for a project."""
    response = client._client.get(
        url=f"{client.base_url}/api/v2/accounts/{client.account_id}/environments/",
        params={"project_id": project_id, "dbt_version__isnull": False},
        headers=client._headers,
    )
    response.raise_for_status()
    environments = response.json().get("data", [])
    for env in environments:
        if env.get("deployment_type") == "production":
            return env["id"]
    for env in environments:
        if "production" in (env.get("name") or "").lower():
            return env["id"]
    for env in environments:
        if env.get("type") == "deployment":
            return env["id"]
    raise ValueError(f"No production environment found for project {project_id}")


def _read_project_name(path: Path) -> str:
    """Read the project name from dbt_project.yml."""
    project_file = path / "dbt_project.yml"
    data = load_yaml(project_file)
    name = data.get("name")
    if not name:
        raise ValueError(f"Could not find project name in {project_file}")
    return str(name)


def _find_model_yamls(path: Path) -> list[tuple[Path, str]]:
    """Find YAML files under models/ that contain a 'models:' key; return (path, content) pairs."""
    models_dir = path / "models"
    if not models_dir.exists():
        return []
    results = []
    for yml_file in models_dir.rglob("*.yml"):
        text = yml_file.read_text()
        if "models:" in text:
            results.append((yml_file, text))
    return results


def configure_sao(
    account_id: int,
    api_key: str,
    base_url: str,
    metadata_url: Optional[str],
    project_id: int,
    environment_id: Optional[int],
    path: Path,
    dry_run: bool,
    json_output: bool,
) -> None:
    """Auto-configure SAO build_after configs from dbt Cloud job history."""
    dbt_client = DBTClient(account_id=account_id, api_key=api_key, base_url=base_url)
    resolved_metadata_url = metadata_url or metadata_url_from_base(base_url)
    discovery_client = DiscoveryClient(api_key=api_key, metadata_url=resolved_metadata_url)

    if environment_id is None:
        console.print("Resolving production environment...")
        environment_id = resolve_prod_environment(dbt_client, project_id)
        console.print(f"Using environment {environment_id}")

    project_name = _read_project_name(path)
    console.print(f"Project: {project_name}")

    jobs = dbt_client.get_jobs_for_sao(project_id=project_id, environment_id=environment_id)

    eligible_jobs: list[tuple[dict, dict]] = []
    for job in jobs:
        if job.get("job_type") != "scheduled":
            continue
        run = job.get("most_recent_run") or job.get("most_recent_completed_run")
        if not run or not run.get("is_success"):
            continue
        eligible_jobs.append((job, run))

    console.print(f"Found {len(eligible_jobs)} eligible scheduled jobs")

    model_sao_configs: dict[str, dict] = {}
    for job, run in eligible_jobs:
        cron = job.get("schedule", {}).get("cron", "0 0 * * *")
        build_after = cron_to_build_after(cron)
        try:
            model_names = discovery_client.get_job_models(job["id"], run["id"], project_name)
        except Exception as exc:
            logging.warning(f"Could not fetch models for job {job['id']}: {exc}")
            continue

        for model_name in model_names:
            existing = model_sao_configs.get(model_name)
            if existing is None or _build_after_minutes(build_after) < _build_after_minutes(existing):
                model_sao_configs[model_name] = build_after

    console.print(f"Mapped {len(model_sao_configs)} models to build_after configs")

    changed_files = []
    for yml_path, yml_str in _find_model_yamls(path):
        new_str, changed = changeset_add_sao_config(yml_str, model_sao_configs)
        if changed:
            changed_files.append(yml_path)
            if not dry_run:
                yml_path.write_text(new_str)

    if json_output:
        print(
            json.dumps(
                {
                    "eligible_jobs": len(eligible_jobs),
                    "models_mapped": len(model_sao_configs),
                    "files_changed": [str(f) for f in changed_files],
                    "dry_run": dry_run,
                },
                indent=2,
            )
        )
    else:
        label = "[dry-run] Would modify" if dry_run else "Modified"
        console.print(f"{label} {len(changed_files)} file(s):")
        for f in changed_files:
            console.print(f"  {f}")
