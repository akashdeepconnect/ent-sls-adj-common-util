import boto3
from boto3.session import Session

def get_cross_account_session(ARN):
    try:
       
        sts_connection = boto3.client("sts")
        dl_acc = sts_connection.assume_role(
            RoleArn=ARN,
            RoleSessionName="cross_acct_lambda"
        )
        ACCESS_KEY = dl_acc["Credentials"]["AccessKeyId"]
        SECRET_KEY = dl_acc["Credentials"]["SecretAccessKey"]
        SESSION_TOKEN = dl_acc["Credentials"]["SessionToken"]
        session = Session(aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          aws_session_token=SESSION_TOKEN)
        return session
        
    except Exception as ex:
        raise ex