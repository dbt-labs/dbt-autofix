"""SQL/Python model changeset for injecting SAO config() block."""
from __future__ import annotations

import re


def _format_freshness_arg(build_after: dict) -> str:
    count = build_after["count"]
    period = build_after["period"]
    return f'freshness={{"build_after": {{"count": {count}, "period": "{period}", "updates_on": "all"}}}}'


def _format_new_config_block(build_after: dict) -> str:
    # Double braces in f-string to produce literal {{ and }}
    return f"{{{{ config(\n    {_format_freshness_arg(build_after)}\n) }}}}"


# Matches {{ config(...) }} or {{- config(...) -}} etc., including multiline
_CONFIG_BLOCK_RE = re.compile(
    r"\{\{-?\s*config\s*\(.*?\)\s*-?\}\}",
    re.DOTALL | re.IGNORECASE,
)

# Matches the closing )) }} of the config call within a block
_CLOSING_RE = re.compile(r"\)\s*-?\}\}")


def changeset_add_sao_config_to_sql(
    file_str: str,
    build_after: dict,
) -> tuple[str, bool]:
    """Add or update a {{ config() }} block in a SQL or Python model file.

    - If no config block exists: prepend one.
    - If a config block exists without freshness: add the freshness parameter.
    - If a config block already has freshness: leave unchanged (idempotent).

    Returns (new_str, changed).
    """
    match = _CONFIG_BLOCK_RE.search(file_str)

    if match:
        config_block = match.group(0)
        if "freshness" in config_block:
            return file_str, False  # Already configured — idempotent

        # Find the closing ) }} within the block
        end_paren = _CLOSING_RE.search(config_block)
        if not end_paren:
            return file_str, False

        # Determine separator: add ", " only if there are existing args
        open_paren = config_block.index("(")
        inner = config_block[open_paren + 1 : end_paren.start()].strip()
        sep = ", " if inner else ""

        freshness_str = f"{sep}{_format_freshness_arg(build_after)}"
        new_config_block = config_block[: end_paren.start()] + freshness_str + config_block[end_paren.start() :]
        new_str = file_str[: match.start()] + new_config_block + file_str[match.end() :]
        return new_str, True

    else:
        # No config block — prepend one with a blank line separator
        config_block = _format_new_config_block(build_after)
        new_str = config_block + "\n\n" + file_str
        return new_str, True
