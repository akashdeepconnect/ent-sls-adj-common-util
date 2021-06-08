from aws_cdk import aws_lambda as lambdas
from aws_cdk import core, aws_iam
import os
from .iam_service import IAMService
import aws_cdk.aws_dynamodb as dynamodb
from aws_cdk import aws_dynamodb as ddb
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
class LambdaService:

    def __init__(self, layers=None, role_arn=None, env=None, scope=None):
        self.layers = None
        self.role = None
        self.scope = scope if scope else None
        self.env = env if env else None
        if layers:
            self.layers = list()
            for layer in layers:
                self.layers.append(
                lambdas.LayerVersion.from_layer_version_arn(
                    scope=scope, id=layer['name'], layer_version_arn=layer['arn']
                ))

        if role_arn:
            self.role = IAMService().role_from_arn(scope=scope, name = 'Lambda Execution Role', role_arn=role_arn)


    # @staticmethod
    def create_lambda_function(self, name, functions_folder, memory, timeout_seconds, scope=None,
                               layers = None, role = None, function_path=None, environment=None):
        scope = scope if scope else self.scope
        lambda_name =  self.env + '_' + name if self.env else name
        layers = layers if layers else self.layers
        role = role if role else self.role
        function_path = function_path if function_path else os.path.join(functions_folder, name)
        return lambdas.Function(
            scope = scope,
            id=lambda_name,
            function_name=lambda_name,
            role=role,
            layers=layers,
            memory_size=memory,
            timeout=core.Duration.seconds(timeout_seconds),
            runtime=lambdas.Runtime.PYTHON_3_7,
            code=lambdas.Code.from_asset(path=function_path),
            handler="lambda_function.lambda_handler",
            environment=environment
        )
    def ret_dynamodb(self):
        return ddb.ITable.table_arn('arn:aws:dynamodb:us-east-1:315207712355:table/test')


    def create_dynamodb_trigger(self):
        db_trigger = DynamoEventSource(self.ret_dynamodb(), batch_size=1000,\
                                       max_batching_window=1,\
                                       max_record_age=60, parallelization_factor=4,\
                                       retry_attempts=0)
        return db_trigger


    def create_lambda_layer(self, name, path, scope=None, description = None, layer_version_name=None):
        scope = scope if scope else self.scope
        if scope is None:
            raise Exception
        ac = lambdas.AssetCode(path=path)
        return lambdas.LayerVersion(scope=scope, id=name, code=ac,description=description, layer_version_name=layer_version_name)