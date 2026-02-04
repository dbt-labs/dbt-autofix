---
name: add-integration-test
description: Create a new integration test for dbt-autofix with proper folder structure and golden files
disable-model-invocation: true
argument-hint: [project-name]
allowed-tools: Read, Write, Bash, Glob, Grep, Edit
---

# Add Integration Test

Create a new integration test for dbt-autofix. Integration tests verify that the refactor tool correctly transforms dbt projects.

## Philosophy: Test-Driven Development

**Integration tests document DESIRED behavior, not current behavior.**

- For bug fixes: Create a test that reproduces the bug, with expected output showing the correct behavior
- For feature requests: Create a test that specifies what the feature should do
- Tests should FAIL initially, then PASS once the fix/feature is implemented

This approach:
1. Serves as a specification for the work
2. Prevents regressions
3. Automatically validates when the implementation is complete

**Using GOLDIE_UPDATE:** Run with `GOLDIE_UPDATE=1` to understand *current* behavior, but don't blindly accept it as the expected output. Manually craft the `_expected` files to reflect what the behavior *should* be.

## Arguments

- `$ARGUMENTS` - The test project name, passed as positional argument to `uv run pytest tests/integration_tests`

**Naming guidance:** Use descriptive names that explain what the test covers, not issue numbers. For example:
- `config_quoted_strings` (not `issue_221`)
- `python_model_meta_config` (not `issue_220`)
- `jinja_unmatched_endif`

The name becomes `project_<name>` in the test projects directory.

## Test Structure

Each integration test requires 3 artifacts in `tests/integration_tests/dbt_projects/`:

1. **`project_<name>/`** - Input dbt project (before refactor)
   - Minimum: `dbt_project.yml` + model files in `models/`

2. **`project_<name>_expected/`** - Expected output (after refactor)
   - Mirror of input with the DESIRED transformations applied

3. **`project_<name>_expected.stdout`** - Expected JSON log output
   - One JSON object per line documenting refactors that SHOULD be applied

## Steps to Follow

### Step 1: Gather Information

**Reference the README** for the authoritative list of deprecations: [README.md](../../../README.md)

**Discover existing test projects** to see patterns and avoid duplication:
```bash
ls tests/integration_tests/dbt_projects/ | grep -v _expected
```

Ask the user:
1. What deprecation, bug, or behavior is being tested?
2. Is there a GitHub issue number to reference?
3. Does this test need special flags?
   - `--behavior-change` mode
   - `--semantic-layer` mode

### Step 2: Understand Current Behavior (Optional)

Run with `GOLDIE_UPDATE=1` to see what the tool currently does:

```bash
GOLDIE_UPDATE=1 .venv/bin/python -m pytest "tests/integration_tests/test_full_dbt_projects.py::test_project_refactor[project_<name>]" -v
```

**Note:** Do NOT commit the auto-generated `_expected` files from `GOLDIE_UPDATE` without review. They reflect current behavior, not necessarily desired behavior. Manually craft the expected output to reflect what the behavior *should* be.

### Step 3: Create Project Structure

```
tests/integration_tests/dbt_projects/
├── project_<name>/
│   ├── dbt_project.yml
│   └── models/
│       └── <model>.sql (or .py for Python models)
├── project_<name>_expected/
│   ├── dbt_project.yml
│   └── models/
│       └── <model>.sql
└── project_<name>_expected.stdout
```

**Minimal `dbt_project.yml`:**
```yaml
name: '<test name>'
version: '1.0.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
```

### Step 4: Create Test Models (Input)

Create model files that demonstrate the behavior being tested. The input should contain the pattern that triggers the deprecation/refactor.

### Step 5: Create Expected Output (Desired Behavior)

Manually create the `_expected` files showing what the output SHOULD be after the fix/feature is implemented. Do NOT just copy current behavior.

### Step 6: Create Expected stdout

Write the JSON log output that SHOULD be produced. Each line is a JSON object:

```json
{"mode": "applied", "file_path": "...", "refactors": [{"deprecation": "...", "log": "..."}]}
{"mode": "complete"}
```

### Step 7: Handle Special Modes (if needed)

If the test requires `--behavior-change` or `--semantic-layer`, add an entry to the dicts in `tests/integration_tests/test_full_dbt_projects.py`:

```python
project_dir_to_behavior_change_mode["project_<name>"] = True
# or
project_dir_to_semantic_layer_mode["project_<name>"] = True
```

### Step 8: Verify Test Behavior

```bash
# Should fail (or xfail) if feature not implemented
.venv/bin/python -m pytest "tests/integration_tests/test_full_dbt_projects.py::test_project_refactor[project_<name>]" -v

# Should pass once feature is implemented
```

## Golden File Tips

- `GOLDIE_UPDATE=1` shows current behavior - use for understanding, not as source of truth
- The `file_path` key is ignored during comparison (paths don't need to match)
- Blank lines are ignored in file comparisons
- The `_expected` suffix must match exactly
- Include trailing newlines in files

## Example Workflows

### Bug Fix (quoted strings causing errors)

1. Create `project_config_quoted_strings/` with a model containing the problematic quoted string
2. Create `project_config_quoted_strings_expected/` showing correct handling (no error, proper transformation)
3. Test fails initially (bug exists)
4. Fix the bug
5. Test passes (bug fixed)

### Feature Request (Python model custom config access)

1. Create `project_python_model_meta_config/` with a Python model using `dbt.config.get("custom_key")`
2. Create `project_python_model_meta_config_expected/` showing the Python code updated to `dbt.config.get("meta").get("custom_key")`
3. Test fails initially (feature not implemented)
4. Implement feature
5. Test passes (feature complete)
