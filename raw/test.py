#%%
import os
from dotenv import load_dotenv
import boto3
import sqlalchemy
import pandas as pd
from tqdm import tqdm

#%%

sqlalchemy.__version__

#%%
load_dotenv()
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
#%%

engine = sqlalchemy.create_engine("sqlite:///../data/gc.db")

#%%

table_names = engine.table_names()

#%%

#%%

def save_s3(table, db_con, s3_client):
    df = pd.read_sql(table, db_con)

    filename = f'../data/{table}.csv'

    df.to_csv(filename)

    s3_client.upload_file(
        filename,
        'dudu-bucket',
        f'gc-bricks/raw/gc/full-load/{table}/full_load.csv'
    )
    return {'status': 'Success'}

#%%

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
#%%

s3_client.list_buckets()

#%%
for table in tqdm(table_names):
    t = table_names[0]
    save_s3(t, engine, s3_client)
#%%



#%%

#%%


