"""Kết nối Cloud SQL. Thông tin đăng nhập lấy từ biến môi trường / file .env,
KHÔNG ghi trong code."""
import os

import pandas as pd
import sqlalchemy
from google.cloud.sql.connector import Connector

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường {name} (xem .env.example)")
    return value


def get_engine(db_name):
    connector = Connector()

    def getconn():
        return connector.connect(
            _env("INSTANCE_CONNECTION_NAME"), "pymysql",
            user=_env("DB_USER"), password=_env("DB_PASS"), db=db_name)

    return sqlalchemy.create_engine("mysql+pymysql://", creator=getconn)


def upsert(engine, table, data, key_cols):
    """Xoá các dòng trùng khoá với `data` rồi append, trong một transaction.

    Không xoá cả bảng như `if_exists='replace'`; dữ liệu cũ không bị đụng tới.
    """
    insp = sqlalchemy.inspect(engine)
    with engine.begin() as conn:
        if not insp.has_table(table):
            data.to_sql(table, conn, index=False)
            return len(data)
        keys = data[key_cols].drop_duplicates()
        tmp = f"_tmp_{table}"
        keys.to_sql(tmp, conn, if_exists='replace', index=False)
        on = " AND ".join(f"t.`{c}` = k.`{c}`" for c in key_cols)
        conn.execute(sqlalchemy.text(
            f"DELETE t FROM `{table}` t JOIN `{tmp}` k ON {on}"))
        conn.execute(sqlalchemy.text(f"DROP TABLE `{tmp}`"))
        data.to_sql(table, conn, if_exists='append', index=False)
    return len(data)


def replace(engine, table, data):
    """Dành cho bảng snapshot (cổ đông) — luôn thay toàn bộ."""
    with engine.begin() as conn:
        data.to_sql(table, conn, if_exists='replace', index=False)
