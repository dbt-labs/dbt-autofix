from pathlib import Path
from tempfile import TemporaryDirectory

from dbt_fusion_package_tools.compatibility import FusionConformanceResult, FusionLogMessage, ParseConformanceLogOutput
from dbt_fusion_package_tools.scripts.package_hub_fusion_compatibility import (
    write_conformance_output_to_json,
)


def test_write_conformance_output_to_json():
    test_data: dict[str, dict[str, FusionConformanceResult]] = {
        "package1": {
            "1.1.1": FusionConformanceResult(
                version="1.1.1",
                require_dbt_version_defined=True,
                require_dbt_version_compatible=False,
                parse_compatible=False,
                parse_compatibility_result=ParseConformanceLogOutput(
                    parse_exit_code=10,
                    total_errors=2,
                    total_warnings=0,
                    errors=[
                        FusionLogMessage("error 1", 1060, "ERROR"),
                        FusionLogMessage("error 2", 8999, "ERROR"),
                    ],
                ),
                manually_verified_compatible=False,
                manually_verified_incompatible=False,
            )
        }
    }
    with TemporaryDirectory() as tmpdir:
        write_conformance_output_to_json(test_data, Path(tmpdir))
