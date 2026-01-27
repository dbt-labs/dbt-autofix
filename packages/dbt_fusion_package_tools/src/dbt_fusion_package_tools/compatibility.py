from dataclasses import dataclass, field
from typing import Optional

from mashumaro.mixins.json import DataClassJSONMixin


@dataclass
class FusionCompatibility(DataClassJSONMixin):
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    fusion_parse: Optional[bool] = None
    dbt_verified: Optional[bool] = None


@dataclass
class FusionLogMessage(DataClassJSONMixin):
    body: str
    error_code: Optional[int] = None
    severity_text: Optional[str] = None
    """Original severity before user up/down-grade configuration applied."""
    original_severity_text: Optional[str] = None


@dataclass
class ParseConformanceLogOutput(DataClassJSONMixin):
    parse_exit_code: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    errors: list[FusionLogMessage] = field(default_factory=list)
    warnings: list[FusionLogMessage] = field(default_factory=list)
    fusion_version: str = "unknown"


@dataclass
class FusionConformanceResult(DataClassJSONMixin):
    version: Optional[str] = None
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    parse_compatible: Optional[bool] = None
    parse_compatibility_result: Optional[ParseConformanceLogOutput] = None
    manually_verified_compatible: Optional[bool] = None
    manually_verified_incompatible: Optional[bool] = None
    download_failed: bool = False
