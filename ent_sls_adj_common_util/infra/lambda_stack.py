from os import path

from aws_cdk import core
import aws_cdk.aws_lambda as lmb
import aws_cdk.aws_codedeploy as codedeploy
import aws_cdk.aws_cloudwatch as cloudwatch
from .configuration import EnvSpecific
import aws_cdk.aws_stepfunctions as stepfunctions
import aws_cdk.aws_stepfunctions_tasks as tasks
from typing import Any, Dict, Type, TypeVar
from aws_cdk import aws_iam
from aws_cdk import aws_events
from .services.lambda_service import LambdaService
import os

class CorepcodeStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, raw_config: EnvSpecific, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        this_dir = path.dirname(__file__)
        functions_folder = path.join(os.path.dirname(this_dir), 'functions')

        lambda_service = LambdaService(role_arn=raw_config.lambda_info['lambda_execution_arn'],
                                       layers=None,
                                       env=raw_config.lambda_info['lambda_env_vars']['ENV'], scope=self)



        get_changed_agencies_info_handler = lambda_service.create_lambda_function(
            name='company_rep',
            functions_folder=functions_folder,
            memory=1024,
            timeout_seconds=600,
            environment=raw_config.lambda_info['lambda_env_vars']

        )

