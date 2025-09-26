# Misspelled config keys

## PROBLEM

In the new authoring layer, new unsupported dbt configs need to be moved into `meta:`, or the Fusion will fail to parse the user project.

This is fine and well, except when a user has misspelled a real config (e.g. `materailized:` instead of `materialized:`).

## SOLUTION

The answer is to fix the config's spelling!

Before moving any custom configs into `meta:` check to see if they are misspellings of real configurations. If so, correct the spelling to that config.

Using the available keys in the JSON schema can be helpful here.
