import json
import os

import boto3
from dotenv import load_dotenv

load_dotenv()


def get_secret_from_aws(secret_name: str, region: str):
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        print(f"[WARN] No se pudo obtener el secreto {secret_name}: {e}")
        return {}


def get_env_var(key: str, default=None, secret_name=None):
    value = os.getenv(key)
    if value is not None:
        return value
    if secret_name:
        region = os.getenv("AWS_REGION")
        secrets = get_secret_from_aws(secret_name, region)
        return secrets.get(key, default)
    return default


AWS_REGION = get_env_var("AWS_REGION")
AWS_ACCESS_KEY_ID = get_env_var("AWS_ACCESS_KEY_ID", secret_name="")
AWS_SECRET_ACCESS_KEY = get_env_var("AWS_SECRET_ACCESS_KEY", secret_name="")
AWS_S3_BUCKET = get_env_var("AWS_S3_BUCKET")

REDIS_HOST = get_env_var("REDIS_HOST")
REDIS_PORT = get_env_var("REDIS_PORT")
