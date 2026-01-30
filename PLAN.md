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

**Ruff version: 0.14.14**

_457 total violations across 31 rules_

### Safe autofix (`uvx ruff check --fix`)
- [x] D212: 62 - Multi-line docstring summary should start at the first line
- [x] I001: 23 - Import block is unsorted or unformatted
- [x] F401: 15 - Unused import
- [x] F541: 5 - f-string without placeholders
- [x] RUF021: 2 - Parenthesize `a and b` expressions when chaining `and` and `or`
- [x] RUF100: 2 - Unused `noqa` directive
- [x] D209: 1 - Multi-line docstring closing quotes should be on a separate line
- [x] PLR5501: 1 - Use `elif` instead of `else` then `if`

### Unsafe autofix (`uvx ruff check --fix --unsafe-fixes`)
- [x] D415: 161 - First line should end with a period, question mark, or exclamation point
- [x] T201: 13 - `print` found (has exceptions - see Known exceptions)
- [x] D200: 11 - One-line docstring should fit on one line
- [x] E711: 5 - Comparison to `None` should be `is None`
- [x] F841: 5 - Local variable is assigned but never used
- [x] PLR1714: 3 - Consider merging multiple comparisons
- [x] T203: 3 - `pprint` found
- [x] E712: 2 - Comparison to `True`/`False` should be `if cond:`
- [x] D301: 1 - Use `r"""` if any backslashes in a docstring
- [x] PLR1722: 1 - Use `sys.exit()` instead of `exit()`
- [x] RUF015: 1 - Prefer `next(iter(...))` over `list(...)[0]`

### Manual fix required
- [ ] D205: 54 - 1 blank line required between summary line and description
- [ ] PLR2004: 15 - Magic value used in comparison
- [ ] E722: 9 - Do not use bare `except`
- [ ] PLC0415: 8 - `import` should be at the top of the file
- [ ] D417: 5 - Missing argument descriptions in the docstring
- [ ] E721: 5 - Use `is` and `is not` for type comparisons
- [ ] PLC0206: 4 - Extracting value from dictionary without `.items()`
- [ ] PLW1641: 2 - Object does not implement `__hash__` method

### Ignored (configured in pyproject.toml)
- PLR0911: Too many return statements
- PLR0912: Too many branches
- PLR0913: Too many arguments in function definition
- PLR0915: Too many statements
