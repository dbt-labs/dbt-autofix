# Pre-commit installs `dbt-fusion-package-tools` from PyPI, not locally

## Conclusion

When `pre-commit try-repo .` builds and installs `dbt-autofix`, the
`dbt-fusion-package-tools` dependency is resolved from **PyPI** — not from
the local workspace at `packages/dbt_fusion_package_tools/`.

This happens because:

1. **`uv sync`** (nox venv setup) respects `[tool.uv.sources]` which maps
   `dbt-fusion-package-tools` to the local workspace member. This is the
   _only_ context where the local copy is used.

2. **`pre-commit try-repo .`** builds a wheel via `pdm-backend`, then installs
   it with `pip` in an isolated temp venv. pip does not know about uv
   workspaces. The `pdm_build.py` hook detects a non-release version
   (`0.0.post1`) and leaves the dependency **unpinned** as bare
   `dbt-fusion-package-tools`, which pip resolves from PyPI.

3. Although `[tool.pdm.build] includes` bundles the source files into the
   wheel at `packages/dbt_fusion_package_tools/src/...`, this path is not
   importable as `import dbt_fusion_package_tools` — it's just unused code
   in the wheel.

## Reproducing

This branch adds logging to verify what in the heck is going on here. Run:

```bash
uvx nox -s test_pre_commit_installation-3.11
```

The output contains three signals proving PyPI resolution:

**Signal A — build-time dependency list** (at the end of the nox output):

```
nox > pdm_build debug log:
version=0.0.post1 is_release=False
dependencies=[..., 'dbt-fusion-package-tools']
```

The dependency is unpinned (no `==` version), so pip resolves it from PyPI.

**Signal B — runtime import path** (in the pre-commit verbose output):

```
[DEBUG] dbt_fusion_package_tools loaded from: /var/folders/.../site-packages/dbt_fusion_package_tools/__init__.py
```

The `site-packages/` path confirms a standard pip install, not a local workspace path.

**Signal C — local marker not present**:

```
[DEBUG] _LOCAL_DEV_MARKER present: False (True=local, False=PyPI)
```

This branch adds `_LOCAL_DEV_MARKER = True` to the local
`packages/dbt_fusion_package_tools/src/dbt_fusion_package_tools/__init__.py`.
The installed version does **not** have it — proving it came from PyPI,
not from the local source.
