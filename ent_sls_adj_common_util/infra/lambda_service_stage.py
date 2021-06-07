from aws_cdk import core
from .configuration import RawConfig, EnvSpecific
from .lambda_stack import CorepcodeStack
from typing import Any, Dict, Type, TypeVar
class CorepcodeService(core.Stage):
  def __init__(self, scope: core.Construct, id: str, raw_config: EnvSpecific, **kwargs):
    super().__init__(scope, id, **kwargs)

    service = CorepcodeStack(self, 'corepcodeservice', raw_config)