import logging
from os import environ

from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO)


def get_client():
    endpoint = f"{environ['MINIO_HOST']}:{environ['MINIO_PORT']}"
    return Minio(
        endpoint=endpoint, access_key=environ["MINIO_ACCESS_KEY"], secret_key=environ["MINIO_SECRET_KEY"], secure=False
    )


def init_storage(client: Minio, bucket: str):
    found = client.bucket_exists(bucket_name=bucket)
    if not found:
        client.make_bucket(bucket)
        logging.info("Bucket created: %s", bucket)
    else:
        logging.info("Bucket %s already exists", bucket)


def get_file(client: Minio, m_bucket: str, m_object: str, m_file: str):
    try:
        client.fget_object(m_bucket, m_object, m_file)
    except S3Error:
        logging.exception(f"Error occurred while downloading file: {m_file}")
        raise
    # if not result.object_name:
    #     raise Exception(f"Object {m_object} was not found in the bucket {m_object}")


def put_file(client: Minio, m_bucket: str, m_object: str, m_file: str):
    try:
        # NOTE: currently it overwrites dupes, aligned with the main application's behavior
        client.fput_object(m_bucket, m_object, m_file)
    except Exception as e:
        logging.exception(f"Error occurred while uploading file: {m_file}: {e}")
        raise
