---
name: add-integration-test
description: Create a new integration test for dbt-autofix with proper folder structure and golden files
disable-model-invocation: true
argument-hint: [project-name]
allowed-tools: Read, Write, Bash, Glob, Grep, Edit
---

# Add Integration Test

Create a new integration test for dbt-autofix. Integration tests verify that the refactor tool correctly transforms dbt projects.

## Arguments

- `$ARGUMENTS` - The test project name (e.g., `issue_123` becomes `project_issue_123`)

## Test Structure

Each integration test requires 3 artifacts in `tests/integration_tests/dbt_projects/`:

1. **`project_<name>/`** - Input dbt project (before refactor)
   - Minimum: `dbt_project.yml` + model files in `models/`

2. **`project_<name>_expected/`** - Expected output (after refactor)
   - Mirror of input with expected transformations applied

3. **`project_<name>_expected.stdout`** - Expected JSON log output
   - One JSON object per line documenting refactors applied

## Steps to Follow

### Step 1: Gather Information

Ask the user:
1. What deprecation, bug, or behavior is being tested?
2. Is there a GitHub issue number to reference?
3. Does this test need special flags?
   - `--behavior-change` mode
   - `--semantic-layer` mode

### Step 2: Create Project Structure

```
tests/integration_tests/dbt_projects/
├── project_<name>/
│   ├── dbt_project.yml
│   └── models/
│       └── <model>.sql
├── project_<name>_expected/
│   ├── dbt_project.yml
│   └── models/
│       └── <model>.sql
└── project_<name>_expected.stdout
```

**Minimal `dbt_project.yml`:**
```yaml
name: test_project
version: "1.0"
```

### Step 3: Create Test Models

Create SQL model files that demonstrate the behavior being tested. The input model should contain the pattern that triggers the deprecation/refactor.

### Step 4: Create Expected Output

Copy the input project to `_expected` and manually apply the expected transformations.

### Step 5: Generate Expected stdout

Run the test with `GOLDIE_UPDATE=1` to auto-generate the `.stdout` file:

```bash
GOLDIE_UPDATE=1 .venv/bin/python -m pytest "tests/integration_tests/test_full_dbt_projects.py::test_project_refactor[project_<name>]" -v
```

### Step 6: Handle Special Modes (if needed)

If the test requires `--behavior-change` or `--semantic-layer`, add an entry to the dicts in `tests/integration_tests/test_full_dbt_projects.py`:

```python
project_dir_to_behavior_change_mode["project_<name>"] = True
# or
project_dir_to_semantic_layer_mode["project_<name>"] = True
```

### Step 7: Verify Test Passes

```bash
.venv/bin/python -m pytest "tests/integration_tests/test_full_dbt_projects.py::test_project_refactor[project_<name>]" -v
```

## Golden File Tips

- Set `GOLDIE_UPDATE=1` env var to regenerate `.stdout` files
- The `file_path` key is ignored during comparison (paths don't need to match)
- Blank lines are ignored in file comparisons
- The `_expected` suffix must match exactly

## Example Workflow

For a bug reported in issue #221 about quoted strings in configs:

1. Create `project_issue_221/` with a model containing the problematic pattern
2. Create `project_issue_221_expected/` with the correctly refactored output
3. Run with `GOLDIE_UPDATE=1` to generate stdout
4. Verify test passes
5. Commit all 3 artifacts
