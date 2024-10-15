from io import BytesIO

import boto3
import pandas as pd
from sqlalchemy import create_engine

from creds import credentials

# credenciais AWS
aws_access_key = credentials["aws_access_key_id"]
aws_secret_key = credentials["aws_secret_access_key"]
region_name = credentials["region_name"]

# bucket S3 e arquivo
bucket_name = credentials["bucket_name"]
file_prefix = credentials["file_prefix"]
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)

# conexão RDS
database_name = credentials["database_name"]
table_name = credentials["table_name"]
rds_host = credentials["rds_host"]
rds_port = credentials["rds_port"]
rds_user = credentials["rds_user"]
rds_password = credentials["rds_password"]


# carrega arquivos CSV em dataframes pandas
def load_csv(s3_bucket, s3_prefix):
    file_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)[
        "Contents"
    ]
    dfs = []
    for file_object in file_objects:
        file_key = file_object["Key"]
        file_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
        df = pd.read_csv(BytesIO(file_obj["Body"].read()))
        dfs.append(df)
    return pd.concat(dfs)


# carrega os arquivos CSV do S3 em um DataFrame
df = load_csv(bucket_name, file_prefix)

# Connect to RDS
conn_str = (
    f"mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{database_name}"
)
engine = create_engine(conn_str)

# escreve o DataFrame no RDS
df.to_sql(table_name, con=engine, if_exists="replace", index=False)

# fecha a conexão
engine.dispose()

print("Finalizado com sucesso!")
