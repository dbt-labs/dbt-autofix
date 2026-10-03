# dbt-migrate-1x

Deterministic fixes for changes introduced _between dbt 1.x minor versions_ (1.3 through the
latest 1.x). This is split out from `dbt-autofix` into its own workspace package, rather than a
`dbt-autofix` subcommand, so it can be invoked directly (e.g. `uv run dbt-migrate-1x`) without
going through the main CLI — this is what the dbt VS Code extension uses to point at it via uv.

It shares `dbt-autofix`'s refactor engine (`dbt_autofix.refactor`, `dbt_autofix.refactors.*`) as a
regular dependency, but never fetches the Fusion JSON schema and needs no network access — see the
root [README](../../README.md#migrate-1x---deterministic-dbt-1x-migrations) for the full rule
coverage table and usage.
