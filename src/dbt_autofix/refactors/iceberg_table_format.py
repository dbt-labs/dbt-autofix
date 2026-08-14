from typing import Any, Optional, Tuple

# Snowflake config keys that only apply to Iceberg tables.
ICEBERG_ONLY_KEYS = ("external_volume", "base_location_root", "base_location_subpath")


def classify_iceberg_table_format(
    table_format: Optional[Any],
    is_iceberg: bool,
    project_has_unsafe_table_format: bool,
) -> Tuple[bool, Optional[str]]:
    """Decide what to do about a config that sets Iceberg-only settings.

    Shared by the SQL, schema-YAML, and dbt_project.yml changesets, which each hold
    the config in a different shape (a statically-parsed source map, a CommentedMap,
    or a nested dict with +prefix inheritance) but make the same decision once they
    know the node's effective ``table_format``.

    Args:
        table_format: The node's table_format value/source, or None if unset.
        is_iceberg: Whether table_format is a literal string equal to "iceberg".
            Ignored when table_format is None.
        project_has_unsafe_table_format: Whether dbt_project.yml sets a non-Iceberg
            table_format that this node could inherit; irrelevant once table_format
            is set here, since it no longer needs to be inherited.

    Returns:
        ``(should_set_iceberg, warning)``: set table_format=iceberg when
        ``should_set_iceberg`` is True; otherwise leave the config untouched, and
        surface ``warning`` (if any) to the user.
    """
    if table_format is not None:
        if is_iceberg:
            return False, None
        return False, f"Cannot safely autofix Iceberg settings with explicit or dynamic table_format={table_format}"
    if project_has_unsafe_table_format:
        return False, (
            "Cannot safely autofix Iceberg settings because dbt_project.yml has a non-literal or non-Iceberg table_format"
        )
    return True, None
