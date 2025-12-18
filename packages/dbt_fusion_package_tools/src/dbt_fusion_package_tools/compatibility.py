from dataclasses import dataclass
from typing import Optional

from mashumaro import DataClassDictMixin


@dataclass
class FusionCompatibility(DataClassDictMixin):
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    fusion_parse: Optional[bool] = None
    dbt_verified: Optional[bool] = None
