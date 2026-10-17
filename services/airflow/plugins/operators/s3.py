import json

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3ReadObjectOperator(BaseOperator):
    """
    Custom Airflow operator to read an object from S3 and push its content to XCom.
    The matching provider version with the frozen airflow version does not have this operator, so we implement it here.
    """

    template_fields = ("s3_bucket", "s3_key")

    def __init__(self, aws_conn_id: str, s3_bucket: str, s3_key: str, **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        content = s3_hook.read_key(bucket_name=self.s3_bucket, key=self.s3_key)
        return json.loads(content)
