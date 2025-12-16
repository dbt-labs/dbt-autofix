"""Utility functions for handling fancy quotes in YAML files."""

# Unique placeholders for fancy quotes that won't conflict with YAML syntax or content
FANCY_LEFT_PLACEHOLDER = "___FANCY_QUOTE_LEFT___"
FANCY_RIGHT_PLACEHOLDER = "___FANCY_QUOTE_RIGHT___"


def restore_fancy_quotes(yml_str: str) -> str:
    """Restore fancy quotes from placeholders in the final YAML output.

    This is the final step after all autofix operations complete. It restores the
    fancy quotes that were temporarily replaced with placeholders.

    Args:
        yml_str: The YAML string with placeholders

    Returns:
        The YAML string with fancy quotes restored
    """
    restored = yml_str.replace(FANCY_LEFT_PLACEHOLDER, "\u201c")
    restored = restored.replace(FANCY_RIGHT_PLACEHOLDER, "\u201d")
    return restored
