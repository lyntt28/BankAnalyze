import os
import pandas as pd
from tqdm import tqdm
from google.cloud.sql.connector import Connector
import sqlalchemy

connector = Connector()
INSTANCE_CONNECTION_NAME = "data-engineer-379601:asia-southeast1:bankstock"
DB_USER = "dashboard"
DB_PASS = "dashboard"

def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db="SEASONAL"
    )
    return conn

pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)
DB_CONN = pool.connect()
_, _, files_name = list(os.walk('results/'))[0]
for file in tqdm(files_name):
    if file=='stock_history.csv':
        continue
    data = pd.read_csv('results/'+file)
    name = file.replace('.csv','')
    data.to_sql(name, DB_CONN, if_exists='replace', index=False)