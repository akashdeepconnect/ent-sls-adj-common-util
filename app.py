#!/usr/bin/env python3

from pathlib import Path
from aws_cdk import core
from ent_sls_adj_common_util.infra.lambda_service_stage import  CorepcodeService
from ent_sls_adj_common_util.infra.pipeline_stack import PipelineStack
from ent_sls_adj_common_util.infra.configuration import RawConfig

config_file = Path('./env_based_resources.json')
raw_config = RawConfig(config_file)


dev_env = core.Environment(**raw_config.development.env)
app = core.App()

PipelineStack(app, 'CorepcodePipelineStack', env={
    'account': '315207712355',
    'region': 'us-east-1'
}, raw_config=raw_config)

app.synth()
