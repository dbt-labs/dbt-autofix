import shutil
import subprocess
import zipfile
from pathlib import Path


def build_wheels(out_dir: Path) -> tuple[Path, Path]:
    """Clear out_dir, build all wheels, assert expected files exist, return (autofix_whl, tools_whl)."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(["uv", "build", "--all", "--out-dir", str(out_dir)], check=True)

    autofix_wheels = sorted(out_dir.glob("dbt_autofix-*.whl"))
    tools_wheels = sorted(out_dir.glob("dbt_fusion_package_tools-*.whl"))
    assert autofix_wheels, "dbt-autofix wheel not found in dist/"
    assert tools_wheels, "dbt-fusion-package-tools wheel not found in dist/"

    return autofix_wheels[-1], tools_wheels[-1]


def inspect_autofix_wheel(whl_path: Path) -> tuple[str, str]:
    """Open wheel zip, assert structural correctness, return (version, tools_dep)."""
    with zipfile.ZipFile(whl_path) as zf:
        wheel_files = zf.namelist()

        hook_files = [f for f in wheel_files if f.startswith("pre_commit_hooks/")]
        assert hook_files, "pre_commit_hooks/ not found in dbt-autofix wheel"

        metadata_files = [f for f in wheel_files if f.endswith("/METADATA")]
        assert metadata_files, "METADATA not found in wheel"
        metadata = zf.read(metadata_files[0]).decode()

        version = None
        for line in metadata.splitlines():
            if line.startswith("Version: "):
                version = line.split(": ", 1)[1]
                break
        assert version, "Version not found in wheel METADATA"

        requires_lines = [line for line in metadata.splitlines() if line.startswith("Requires-Dist:")]
        tools_deps = [line for line in requires_lines if "dbt-fusion-package-tools" in line]
        assert tools_deps, "dbt-fusion-package-tools not found in wheel METADATA.\nRequires-Dist lines:\n" + "\n".join(
            requires_lines
        )

    return version, tools_deps[0]


def make_venv(path: Path) -> Path:
    """Create a uv venv at path, return path to the Python interpreter."""
    subprocess.run(["uv", "venv", str(path)], check=True)
    return path / "bin" / "python"


def install_wheels_in_venv(venv_path: Path, *wheel_paths: Path) -> None:
    """Install wheels into a venv and verify CLI entry points work."""
    python = venv_path / "bin" / "python"
    subprocess.run(
        ["uv", "pip", "install", "--python", str(python), *(str(w) for w in wheel_paths)],
        check=True,
    )
    subprocess.run([str(venv_path / "bin" / "dbt-autofix"), "--help"], check=True)
    subprocess.run(
        [str(python), "-m", "pre_commit_hooks.check_deprecations", "--help"],
        check=True,
    )
