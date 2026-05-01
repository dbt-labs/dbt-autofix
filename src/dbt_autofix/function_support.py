import json
import logging
from pathlib import Path

import httpx

DOCS_FUNCTIONS_URL = (
    "https://raw.githubusercontent.com/dbt-labs/docs.getdbt.com"
    "/main/website/static/data/functions/{adapter}.json"
)

BUNDLED_DATA_PATH = Path(__file__).parent / "data" / "snowflake-functions-bundled.json"


def load_function_support(adapter: str = "snowflake") -> dict:
    """Fetch function support data; falls back to bundled snapshot if offline."""
    url = DOCS_FUNCTIONS_URL.format(adapter=adapter)
    try:
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logging.warning(
            f"Could not fetch function support data from {url}: {exc}. Using bundled snapshot."
        )
        return _load_bundled(adapter)


def _load_bundled(adapter: str = "snowflake") -> dict:
    bundled_path = Path(__file__).parent / "data" / f"{adapter}-functions-bundled.json"
    with open(bundled_path) as f:
        return json.load(f)


def get_unsupported_functions(data: dict) -> set[str]:
    """Return uppercase function names NOT supported in Fusion strict mode."""
    return {
        fn["name"].upper()
        for fn in data.get("functions", [])
        if not fn.get("fusion_typecheck", True)
    }
