# TODO: Need some logging
from os import environ

from minio import Minio
from minio.error import S3Error


def get_client():
    return Minio(
        endpoint=environ["MINIO_HOST"], access_key=environ["MINIO_ACCESS_KEY"], secret_key=environ["MINIO_SECRET_KEY"]
    )


def init_storage(client: Minio, bucket: str):
    found = client.bucket_exists(bucket_name=bucket)
    if not found:
        client.make_bucket(bucket)
        print("Bucket created", bucket)
    else:
        print("Bucket", bucket, "already exists")


def get_file(client: Minio, m_bucket: str, m_object: str, m_file: str):
    try:
        client.fget_object(m_bucket, m_object, m_file)
    except S3Error:
        raise
    # if not result.object_name:
    #     raise Exception(f"Object {m_object} was not found in the bucket {m_object}")


def put_file(client: Minio, m_bucket: str, m_object: str, m_file: str):
    try:
        client.fput_object(m_bucket, m_object, m_file)
    except S3Error:
        raise
