from typing import Any, Optional, Union
from dataclasses import dataclass
import re
from typing import Any, Iterable, List, Union
from enum import Enum
from dbt_fusion_package_tools.exceptions import SemverError, VersionsNotCompatibleError
from mashumaro import DataClassDictMixin
from typing import Optional
from dbt_fusion_package_tools.version_utils import VersionRange

@dataclass
class FusionCompatibility(DataClassDictMixin):
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    fusion_parse: Optional[bool] = None
    dbt_verified: Optional[bool] = None

