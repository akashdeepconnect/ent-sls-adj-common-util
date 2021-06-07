from aws_cdk import core, aws_iam


class IAMService:
    def __init__(self, scope=None):
        self.scope = scope if scope else None

    def role_from_arn(self, scope, name, role_arn):
        scope = scope if scope else self.scope
        return aws_iam.Role.from_role_arn(id=name, role_arn=role_arn, scope=scope)
