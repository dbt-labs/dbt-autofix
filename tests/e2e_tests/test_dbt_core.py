import subprocess

import pytest


@pytest.mark.parametrize("dbt_core_version", ["1.10.6", "1.11.2"])
def test_dbt_core_installation(dbt_core_version):
    subprocess.run(
        ["uv", "run", "--with", f"dbt-core=={dbt_core_version}", "dbt-autofix", "--help"],
        check=True,
    )
