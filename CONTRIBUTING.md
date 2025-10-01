# Contributing to `dbt-autofix`

## Installation

To install `dbt-autofix` locally, simply run `pip install .` in a local python virtual environment. 

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
