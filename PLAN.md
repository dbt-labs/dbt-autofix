# Get caught up on Ruff errors and enforce

## Goals

- Enforce Ruff linting in CI
- Fix all existing lint issues in all Python files in the codebase

## Constraints

- We should not install ruff as a dev dependency (call it as a uv tool)
- We should use an exact version of Ruff - both locally and in github actions

## Methodology

- Identify all rules that have existing violations in the codebase
- Make commits that fix violations one rule at a time. e.g., in this commit we autofix all of rule XYZ. Another commit will manually fix all violations of rule ABC.

## Known exceptions

- We should allow `print` calls in scripts/ and tests/. This is part of our feedback loop from these files.
- Print's in **main** functions are also allowed - but should just be in-line ignored instead of on a file-basis

## Advice

- If you remove dead code, make sure to also remove comments about the dead code!
- If you remove unused variables, make sure to also remove any related comments or what the variable was getting assigned from

## When done

The final PR should not have this plan in it. But I'm adding it to version control at the start so that updating the tracker can be part of each commit.

## Tracker

_Count up all the violations of each rule here, so we can track progress as we fix one rule at a time. Make a checklist. Each item should be a rule. Number of violations, and whether it's autofixable, unsafe-autofixable, or requires manual fixes_

- [ ]
