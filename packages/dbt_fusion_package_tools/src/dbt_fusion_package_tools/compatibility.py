from dataclasses import dataclass, field
from json import dumps
from typing import Any, Optional

from mashumaro import DataClassDictMixin, field_options
from mashumaro.mixins.json import DataClassJSONMixin, Encoder
from dbtlabs.proto.public.v1.events.fusion.invocation.invocation_pb2 import Invocation
from dbtlabs.proto.public.v1.events.fusion.log.log_pb2 import LogMessage
from google.protobuf import json_format


@dataclass
class FusionCompatibility(DataClassJSONMixin):
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    fusion_parse: Optional[bool] = None
    dbt_verified: Optional[bool] = None


@dataclass
class FusionLogMessage(DataClassJSONMixin):
    body: str
    message: str
    # message: LogMessage = field(
    #     metadata=field_options(
    #         serialize=lambda msg: msg.SerializeToString(),
    #     ))
    # def to_dict(self) -> dict[Any, Any]:
    #     return {
    #         "body": self.body,
    #         "message": self.message.SerializeToString()
    #     }


@dataclass
class ParseConformanceLogOutput(DataClassJSONMixin):
    parse_exit_code: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    errors: list[FusionLogMessage] = field(default_factory=list)
    warnings: list[FusionLogMessage] = field(default_factory=list)


@dataclass
class FusionConformanceResult(DataClassJSONMixin):
    version: Optional[str] = None
    require_dbt_version_defined: Optional[bool] = None
    require_dbt_version_compatible: Optional[bool] = None
    parse_compatible: Optional[bool] = None
    parse_compatibility_result: Optional[ParseConformanceLogOutput] = None
    manually_verified_compatible: Optional[bool] = None
    manually_verified_incompatible: Optional[bool] = None
