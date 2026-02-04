"""Transformations for Python dbt models."""

import ast
import re
from typing import List, Set

from dbt_autofix.deprecations import DeprecationType
from dbt_autofix.refactors.results import DbtDeprecationRefactor, SQLRuleRefactorResult
from dbt_autofix.retrieve_schemas import SchemaSpecs


class ConfigGetTransformer(ast.NodeTransformer):
    """AST transformer to convert dbt.config.get() to dbt.meta_get() for custom configs."""

    def __init__(self, allowed_config_fields: Set[str]):
        self.allowed_config_fields = allowed_config_fields
        self.transformations: List[tuple] = []
        self.warnings: List[str] = []
        self.config_shadowed = False

    def visit_Assign(self, node: ast.Assign) -> ast.Assign:
        """Detect if 'config' variable is being shadowed."""
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "config":
                self.config_shadowed = True
        self.generic_visit(node)
        return node

    def visit_Call(self, node: ast.Call) -> ast.Call:
        """Transform dbt.config.get() calls to dbt.meta_get()."""
        # Check if config is shadowed - skip transformation
        if self.config_shadowed:
            return node

        # Check if this is a dbt.config.get() call
        if self._is_dbt_config_get_call(node):
            # Extract the key argument
            if not node.args:
                return node

            key_arg = node.args[0]
            if not isinstance(key_arg, ast.Constant) or not isinstance(key_arg.value, str):
                # Can't statically determine key, skip
                return node

            config_key = key_arg.value

            # Skip if this is a dbt-native config
            if config_key in self.allowed_config_fields:
                return node

            # Check for chained access (e.g., dbt.config.get('key').attr)
            # This is harder to detect in AST transformation, will handle in post-processing

            # Create new node: dbt.meta_get()
            new_func = ast.Attribute(
                value=ast.Name(id='dbt', ctx=ast.Load()),
                attr='meta_get',
                ctx=ast.Load()
            )

            # Copy location info
            ast.copy_location(new_func, node.func)
            ast.copy_location(new_func.value, node.func)

            # Create new call with same arguments
            new_call = ast.Call(
                func=new_func,
                args=node.args.copy(),
                keywords=node.keywords.copy()
            )
            ast.copy_location(new_call, node)

            # Record transformation for logging
            original = self._ast_to_code_snippet(node, config_key)
            replacement = f"dbt.meta_get('{config_key}'{'...' if len(node.args) > 1 or node.keywords else ''})"
            self.transformations.append((original, replacement))

            return new_call

        self.generic_visit(node)
        return node

    def _is_dbt_config_get_call(self, node: ast.Call) -> bool:
        """Check if this is a dbt.config.get() call."""
        if not isinstance(node.func, ast.Attribute):
            return False

        if node.func.attr != 'get':
            return False

        if not isinstance(node.func.value, ast.Attribute):
            return False

        if node.func.value.attr != 'config':
            return False

        if not isinstance(node.func.value.value, ast.Name):
            return False

        if node.func.value.value.id != 'dbt':
            return False

        return True

    def _ast_to_code_snippet(self, node: ast.Call, key: str) -> str:
        """Generate a readable code snippet from the original call."""
        args_parts = [f"'{key}'"]

        # Add positional args after the key
        if len(node.args) > 1:
            args_parts.append('...')

        # Add keyword args
        if node.keywords:
            args_parts.append('...')

        return f"dbt.config.get({', '.join(args_parts)})"


def move_custom_config_access_to_meta_python(
    python_content: str, schema_specs: SchemaSpecs, node_type: str
) -> SQLRuleRefactorResult:
    """Move custom config access to meta in Python files using dbt.meta_get().

    This transforms:
    - dbt.config.get('custom_key') -> dbt.meta_get('custom_key')
    - dbt.config.get('custom_key', 'default') -> dbt.meta_get('custom_key', 'default')

    Only transforms custom configs (those not in allowed_config_fields).
    Preserves defaults and all other arguments.

    Note: Python models only have get(), not require() like SQL models.

    Args:
        python_content: The Python model content to process
        schema_specs: The schema specifications to use
        node_type: The type of node to process

    Returns:
        SQLRuleRefactorResult: Result containing refactored content and metadata
    """
    refactored = False
    refactored_content = python_content
    deprecation_refactors: List[DbtDeprecationRefactor] = []
    refactor_warnings: List[str] = []

    # Parse the Python code
    try:
        tree = ast.parse(python_content)
    except SyntaxError as e:
        refactor_warnings.append(f"Failed to parse Python code: {e}")
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_python",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
            refactor_warnings=refactor_warnings,
        )

    # Get all allowed config fields across all node types
    allowed_config_fields: Set[str] = set()
    for specs in schema_specs.yaml_specs_per_node_type.values():
        allowed_config_fields.update(specs.allowed_config_fields)

    # Transform the AST
    transformer = ConfigGetTransformer(allowed_config_fields)
    new_tree = transformer.visit(tree)

    # Check if config was shadowed
    if transformer.config_shadowed:
        refactor_warnings.append(
            "Detected 'config' variable assignment. Skipping refactor to avoid false positives."
        )
        return SQLRuleRefactorResult(
            rule_name="move_custom_config_access_to_meta_python",
            refactored=False,
            refactored_content=python_content,
            original_content=python_content,
            deprecation_refactors=[],
            refactor_warnings=refactor_warnings,
        )

    # If transformations were made, convert back to code
    if transformer.transformations:
        refactored = True
        try:
            # Unparse the modified AST back to code
            refactored_content = ast.unparse(new_tree)
        except Exception as e:
            refactor_warnings.append(f"Failed to unparse modified AST: {e}")
            return SQLRuleRefactorResult(
                rule_name="move_custom_config_access_to_meta_python",
                refactored=False,
                refactored_content=python_content,
                original_content=python_content,
                deprecation_refactors=[],
                refactor_warnings=refactor_warnings,
            )

        # Record all transformations
        for original, replacement in transformer.transformations:
            deprecation_refactors.append(
                DbtDeprecationRefactor(
                    log=f'Refactored "{original}" to "{replacement}"',
                    deprecation=DeprecationType.CUSTOM_KEY_IN_CONFIG_DEPRECATION,
                )
            )

    # Check for chained access patterns by looking for dbt.meta_get().something
    if refactored:
        # Simple heuristic: look for patterns like ).attr or ).method
        chained_pattern = re.compile(r'dbt\.meta_get\([^)]+\)\s*\.')
        if chained_pattern.search(refactored_content):
            refactor_warnings.append(
                "Detected chained access after dbt.meta_get() call. "
                "These patterns require manual review as the structure may need to be adjusted."
            )

    # Include any warnings from the transformer
    refactor_warnings.extend(transformer.warnings)

    return SQLRuleRefactorResult(
        rule_name="move_custom_config_access_to_meta_python",
        refactored=refactored,
        refactored_content=refactored_content,
        original_content=python_content,
        deprecation_refactors=deprecation_refactors,
        refactor_warnings=refactor_warnings,
    )
