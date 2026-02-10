# Contributing to `dbt-autofix`

## Installation

This project uses `uv` for workspace and virutalenv management.

1. Clone the repo: `gh repo clone dbt-labs/dbt-autofix`
2. Install `uv`: [See docs](https://docs.astral.sh/uv/getting-started/installation/)
3. Install dependencies: `uv sync --extra test`
4. Activate virtual environment: `source .venv/bin/activate`

You confirm your installation worked as expected via `dbt-autofix --version`

## Testing

The tests in this codebase are located in the `tests` directory and contain `integration_tests` and `unit_tests` subdirectories.

To run all tests in the repo, simply run `pytest tests`

Integration tests are based on check-in sample dbt projects and corresponding expected dbt projects and stdout log files. For example:
* `integration_tests/dbt_projects/project1` is a sample input project.
* `integration_tests/dbt_projects/project1_expected` is the expected resulting project after `dbt-autofix deprecations <flags>` is run.
* `integration_tests/dbt_projects/project1_expected.stdout` contains the the expected stdout logs.

If `*.stdout` files need to be updated to reflect new or changed logs as part of the change being introduced, run: 

```sh
GOLDIE_UPDATE=1 pytest tests/integration_tests
```

to automatically update the expected *.stdout files.
## Adding new dependencies

`dbt-autofix` declares `dependencies` as dynamic (managed by the `uv-dynamic-versioning` metadata hook), so `uv add` cannot write to `[project].dependencies`.

- **Runtime dependency:** Manually add it to the `[tool.hatch.metadata.hooks.uv-dynamic-versioning].dependencies` list in `pyproject.toml`, then run `uv sync`.
- **Dev/test dependency:** `uv add --optional test <package>` works as normal since `[project.optional-dependencies]` is static.

## Releasing

To kick off a new release: 
1. Click into 'Releases' under the 'About' section of https://github.com/dbt-labs/dbt-autofix
2. Click 'Draft a new release'
3. Create a new tag - incrementing the last latest release available on https://pypi.org/project/dbt-autofix/ by either a minor or patch version as appropriate - format should be `v0.*.*`
4. Title the release the same as the tag name from (3)
5. Click 'Generate release notes` to populate the release body
6. Hit 'Publish Release' ensuring that `Set as the latest release` is checked.

That's it! The Github release triggers an automated release to pypi which can be observed at: https://github.com/dbt-labs/dbt-autofix/actions/workflows/release.yml

## Troubleshooting

If you're not getting autocomplete in VSCode for `dbt-fusion-package-tools`, open VSC settings and enable "Python > Terminal: Activate Env in Current Terminal."