from aws_cdk import core
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as cpactions
from aws_cdk import pipelines
from .configuration import  RawConfig

from .lambda_service_stage import CorepcodeService


class PipelineStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, raw_config: RawConfig, **kwargs):
        super().__init__(scope, id, **kwargs)
        self._raw_config = raw_config
        source_artifact = codepipeline.Artifact()
        cloud_assembly_artifact = codepipeline.Artifact()

        pipeline = pipelines.CdkPipeline(self, 'Pipeline',
                                         cloud_assembly_artifact=cloud_assembly_artifact,
                                         pipeline_name='CorepcodePipeline',

                                         source_action=cpactions.GitHubSourceAction(
                                             #connection_arn=self._raw_config.application['connection_arn'],
                                             action_name='GitHub',
                                             output=source_artifact,
                                             oauth_token=core.SecretValue.secrets_manager('github-token'),
                                             owner=self._raw_config.application['owner'],
                                             repo=self._raw_config.application['repo_name'],
                                             branch=self._raw_config.application['branch'],
                                             trigger=cpactions.GitHubTrigger.POLL ),

                                         synth_action=pipelines.SimpleSynthAction(
                                             source_artifact=source_artifact,
                                             cloud_assembly_artifact=cloud_assembly_artifact,
                                             install_command='npm install -g aws-cdk && pip install -r requirements.txt',
                                             build_command='pytest Tests',
                                             synth_command='cdk synth'))

        dev_env = core.Environment(**self._raw_config.development.env)

        # print(type(self._raw_config.development), len(self._raw_config.development), self._raw_config.development)
        print(type(self._raw_config.development))
        l2_app = CorepcodeService(self, 'L2', env=dev_env, raw_config=self._raw_config.development)
        l2_stage = pipeline.add_application_stage(l2_app)

        l2_stage.add_manual_approval_action(
            action_name='Promote_To_L3'
        )

        l3_app = CorepcodeService(self, 'L3', env=dev_env,raw_config=self._raw_config.staging)
        l3_stage = pipeline.add_application_stage(l3_app)
        '''l3_stage.add_manual_approval_action(
            action_name='Promote_To_L4'
        )
        prod_env = core.Environment(**self._raw_config.production.env)

        prod_app = AgupService(self, 'L4', env=prod_env, raw_config=self._raw_config.production)
        l4_stage = pipeline.add_application_stage(prod_app)'''
        # pre_prod_stage.add_actions(pipelines.ShellScriptAction(
        #   action_name='Integ',
        #   run_order=pre_prod_stage.next_sequential_run_order(),
        #   additional_artifacts=[source_artifact],
        #   commands=[
        #     'pip install -r requirements.txt',
        #     'pytest integtests',
        #   ],
        #   use_outputs={
        #     'SERVICE_URL': pipeline.stack_output(pre_prod_app.url_output)
        #   }))

        # pipeline.add_application_stage(WebServiceStage(self, 'Prod', env={
        #   'account': APP_ACCOUNT,
        #   'region': 'eu-central-1',
        # }))
