# dbt-autofix

dbt-autofix automatically scans your dbt project for deprecated configurations and updates them to align with the latest best practices. This makes it easier to resolve deprecation warnings introduced in dbt v1.10 as well as prepare for migration to the dbt Fusion engine.

***NEW in version 0.17.0***: dbt-autofix can now check package dependencies for compatibility with dbt Fusion and dbt 2.0 and automatically upgrade packages to newer compatible versions. See `packages` in the `Usage` section below for more detail.

There will also be cases that dbt-autofix cannot resolve and require manual intervention. For those scenarios, using AI Agents can be helpfiul see the below section on [Using `AGENTS.md`](#using-agentsmd). Even if you don't intend to use LLMs, the [`AGENTS.md`](./AGENTS.md) can be a very helpful guidance for work that may need to be done after autofix has done it's part.


## Deprecation Coverage - Project Files

The following deprecations are covered by `dbt-autofix deprecations`:

| Deprecation Code in dbt Core      | Files             | Handling                                                                                         | Support | Behavior Change |
| --------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------ | ------- | --------------- |
| `PropertyMovedToConfigDeprecation`    | YAML files        | Move all deprecated property-level configs under `config:` in YAML files across all resource types (models, exposures, owners, etc)      |   Full  | No |
| `CustomKeyInObjectDeprecation`    | YAML files        | Move all models extra config (not valid or custom) under `meta:` and `meta` under `config:`      |   Full  | No |
| `DuplicateYAMLKeysDeprecation`    | YAML files        | Remove duplicate keys in YAML files, keeping the second one to keep the same behaviour           |   Full  | No |
| `CustomTopLevelKeyDeprecation` | YAML files | Delete custom top-level key-value pairs in YAML files | Full | No |
| `UnexpectedJinjaBlockDeprecation` | SQL files         | Remove extra `{% endmacro %}` and `{% endif %}` that don't have corresponding opening statements |   Full  | No |
| `MissingPlusPrefixDeprecation` | `dbt_project.yml` | Prefix all built-in configs for models/tests etc... with a `+`                                     | Partial (Does not yet prefix custom configs) | No |
| `ConfigDataPathDeprecation`       | `dbt_project.yml` | Remove deprecated config for data path (now seed)                                                |   Full  | No |
| `ConfigLogPathDeprecation`        | `dbt_project.yml` | Remove deprecated config for log path                                                            |   Full  | No |
| `ConfigSourcePathDeprecation`     | `dbt_project.yml` | Remove deprecated config for source path                                                         |   Full  | No |
| `ConfigTargetPathDeprecation`     | `dbt_project.yml` | Remove deprecated config for target path                                                         |   Full  | No |
| `ExposureNameDeprecation` | YAML files | Replaces spaces with underscores and removes non-alphanumeric characters in exposure names | Full | Yes |
| `ResourceNamesWithSpacesDeprecation` | SQL files, YAML files | Replaces spaces with underscores in resource names, updating .sql filenames as necessary | Full | Yes |  
| `SourceFreshnessProjectHooksNotRun` | `dbt_project.yml` | Set `source_freshness_run_project_hooks` in `dbt_project.yml` "flags" to true | Full | Yes |
| `MissingArgumentsPropertyInGenericTestDeprecation` | YAML files | Move any keyword arguments defined as top-level property on generic test to `arguments` property | Full | No |

## Deprecation Coverage - CLI Commands

The following deprecations are covered by `dbt-autofix jobs`:

| Deprecation Code in dbt                        | Handling                              | Support | Behavior Change |
| ---------------------------------------------- | ------------------------------------- | ------- | --------------- |
| `ModelParamUsageDeprecation`                   | Replace -m/--model with -s/--select   |   Full  |       No        |
| `CustomOutputPathInSourceFreshnessDeprecation` | Remove -o/--output usage in `dbt source freshness` commands              |   Full  | Yes |


## Installation

### In dbt Studio

If you are using dbt Studio, no installation is needed. You can run `dbt-autofix` in the Studio command line just like you run other commands like `dbt build`.

### From PyPi

#### With uv (recommended)

We recommend using `uv`/`uvx` to run the package.
If you don't have `uv` installed, you can install `uv` and `uvx`, [following the instructions on the offical website](https://docs.astral.sh/uv/getting-started/installation/).

- to run the latest version of the tool: `uvx dbt-autofix`
- to run a specific version of the tool: `uvx dbt-autofix@0.1.2`
- to install the tool as a dedicated CLI: `uv tool install dbt-autofix`
- to upgrade the tool installed as a dedicated CLI: `uv tool upgrade dbt-autofix`

#### With pip

You can also use `pip` if you prefer, but we then recommend installing the tool in its own Python virtual environment. Once in a venv, install the tool with `pip install dbt-autofix` and then run `dbt-autofix ...` 

### From the source repo

To run it from the git repo directly, install `uv` [following those instructions](https://docs.astral.sh/uv/getting-started/installation/) and then:

run the tool directly
```sh
uvx --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix --help
```

or install it so that it can be run with `dbt-autofix` in the future
```sh
uv tool install --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix
```

## Usage

### `deprecations` - the main one

- `dbt-autofix deprecations`: refactor YAML and SQL files to fix some deprecations
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
  - add `--dry-run` for running in dry run mode
  - add `--json` to get resulting data in a JSONL format
  - add `--json-schema-version v2.0.0-beta.4` to get the JSON schema from a specific Fusion release (by default we pick the latest)
  - add `--select <path>` to only select files in a given path (by default the tool will look at all files of the dbt project)
  - add `--include-packages` to also autofix the packages installed. Just note that those fixes will be reverted at the next `dbt deps` and the long term fix will be to update the packages to versions compatible with Fusion.
  - add `--include-private-packages` to autofix just the _private_ packages (those not on [hub.getdbt.com](https://hub.getdbt.com/)) installed. Just note that those fixes will be reverted at the next `dbt deps` and the long term fix will be to update the packages to versions compatible with Fusion.
  - add `--behavior-change` to run the _subset_ of fixes that would resolve deprecations that require a behavior change. Refer to the coverage tables above to determine which deprecations require behavior changes.
  - add `--all` to run all of the fixes possible - both fixes that potentially require behavior changes as well as not. Additionally, `--all` will apply fixes to as many files as possible, even if some files are unfixable (e.g. due to invalid yaml syntax).

Each JSON object will have the following keys:

- "mode": "applied" or "dry_run" 
- "file_path": the full path of the file modified. Each file will appear only once
- "refactors": the list of refactoring rules applied

Calling `deprecations` without `--dry-run` should be safe if your dbt code is part of a git repo. 

Please review the suggested changes to your dbt project before merging to `main` and make those changes go through your typical CI/CD process.


### `packages` - the new one

- `dbt-autofix packages`: scan package dependencies for compatibility with Fusion and dbt 2.0 and modify packages.yml or dependencies.yml to upgrade any incompatible packages to a newer compatible version
  - add `--force-upgrade` to override the version range currently defined in your project's packages.yml/dependencies.yml
  - add `--path <mypath>` to configure the path of the dbt project (defaults to `.`)
  - add `--dry-run` for running in dry run mode
  - add `--json` to get resulting data in a JSONL format

If any packages are upgraded, you must run `dbt deps` in your project to install the new versions and update your lock file.

Each JSON object will have the following keys:

- "mode": "applied" or "dry_run" 
- "file_path": the full path of the file modified
- "upgrades": the list of packages upgraded to newer versions
- "unchanged": the list of packages not upgraded and the reason for not upgrading, including:
  - Package is already compatible with Fusion and no update is required
  - Package is not compatible with Fusion and Package Hub does not have a newer version with Fusion compatibility
  - Package has not defined Fusion compatibility using `require-dbt-version`

Calling `packages` without `--dry-run` should be safe if your dbt code is part of a git repo. 

Please review the suggested changes to your dbt project before merging to `main` and make those changes go through your typical CI/CD process.

### `jobs`

`dbt-autofix jobs`: update dbt platform jobs steps to use `-s`/`--select` selectors instead of `-m`/`--models`/`--model` which are deprecated in the Fusion engine

Run `dbt-autofix jobs --help` to see the required parameters and supported arguments.

This tool requires connecting to the dbt Admin API to retrieve and update jobs which means that the user token or service token used need to have Read and Write access to jobs

Running with `--dry-run`/`d` will output what changes would have been triggered without triggering them

Running with `--behavior-changes` will run the _subset_ of fixes that would resolve deprecations that require a behavior change. Refer to the coverage tables above to determine which deprecations require behavior changes.

### Using `AGENTS.md`

[`AGENTS.md`](./AGENTS.md) is provided as a reference and starting place for those interested in using AI agents in Cursor, Copilot Chat, and Claude Code to try resolving remaining errors after running dbt-autofix. 

**To use AGENTS.md:**
1. Download AGENTS.md and the /manual_fixes/ directory (you can remove these files after using the agentic autofix workflow)
2. Add AGENTS.md as context to the chat or Claude Code
3. Be very specific in your prompt to provide the proper guardrails and avoid AI hallucinations

**Sample prompt:**

Please make my dbt project compatible with Fusion by strictly following the instructions in AGENTS.md. Please read AGENTS.md and dependent resources in full before you start, and take time planning and thinking through steps.

**Share your manual fixes!**

Have you had to make manual adjustments to get your dbt project working with Fusion? We’d love for you to contribute them back to the community through this agentic workflow!

The `/manual_fixes/` folder is a collection of real examples where users have solved compatibility issues manually, and we would love your contribution to it. Your contribution helps improve autofix for everyone and can prevent others from hitting the same issue. 

### Pre-commit Hooks

You can use `dbt-autofix` as a pre-commit hook to automatically catch and fix deprecations before committing code. 

Add the following to your `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/dbt-labs/dbt-autofix
    rev: v0.13.x # or 'main' or 'HEAD'
    hooks:
      - id: dbt-autofix-check # Check for deprecations without making changes
      # OR
      - id: dbt-autofix-fix # Automatically fix deprecations
      # OR
      - id: dbt-autofix-fix # Pass in multiple args
        args: [--semantic-layer, --include-packages, --behavior-change]
      # OR
      - id: dbt-autofix-fix # Specify dbt project path
        args: [--path=jaffle-shop]
```
