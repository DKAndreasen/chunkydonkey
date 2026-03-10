import os
from aioboto3 import Session

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://seaweedfs:8333")
BUCKET = "chunkydonkey"

session = Session()


async def save(sha256: str, data: bytes, content_type: str = "application/octet-stream"):
    async with session.client("s3", endpoint_url=S3_ENDPOINT_URL) as s3:
        await s3.put_object(Bucket=BUCKET, Key=sha256, Body=data, ContentType=content_type)


async def load(sha256: str) -> bytes | None:
    async with session.client("s3", endpoint_url=S3_ENDPOINT_URL) as s3:
        try:
            resp = await s3.get_object(Bucket=BUCKET, Key=sha256)
            return await resp["Body"].read()
        except s3.exceptions.NoSuchKey:
            return None


async def delete(sha256: str):
    async with session.client("s3", endpoint_url=S3_ENDPOINT_URL) as s3:
        await s3.delete_object(Bucket=BUCKET, Key=sha256)
