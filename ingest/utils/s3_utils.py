import io
import os
import boto3
from fastapi import HTTPException
import pandas as pd
from config import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar el cliente S3: {e}")

def list_gold_files():
    response = s3_client.list_objects_v2(
        Bucket=AWS_S3_BUCKET,
        Prefix="gold/model/"
    )

    if "Contents" not in response:
        return []

    files = [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".parquet")
    ]

    # Retornar el nombre de archivo sin la ruta
    file_names = [f.split("/")[-1] for f in files]
    return file_names


def read_parquet_from_s3(key: str) -> pd.DataFrame:
    try:
        obj = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
        df = pd.read_parquet(io.BytesIO(raw_bytes))
        return df
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {key}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def upload_to_s3(
    data_bytes, key, content_type="application/octet-stream", metadata=None
):
    try:
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=key,
            Body=data_bytes,
            ContentType=content_type,
            Metadata=metadata or {},
        )
        return f"s3://{AWS_S3_BUCKET}/{key}"
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        raise HTTPException(status_code=500, detail=f"Fallo de S3: {str(e)}")